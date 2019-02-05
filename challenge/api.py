from flask import request, jsonify, make_response
from flask import Blueprint
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import func

from challenge.models import Session, Payment, Patient, to_date_format


def delete_models(model_ids, Model):
    session = Session()
    session.query(Model).filter(~Model.id.in_(model_ids)).delete(synchronize_session=False)
    session.commit()


def upsert_model(raw_model, Model):
    session = Session()
    model = Model.create(raw_model)
    session.add(model)
    try:
        session.flush()
    except SQLAlchemyError:
        session.rollback()
        model = session.query(Model).filter_by(external_id=raw_model['externalId']).first()
        model.update(raw_model)
        session.add(model)
        session.flush()

    session.commit()

    return model.id


def payments_get():
    session = Session()

    query = session.query(Payment)
    external_id = request.args.get('external_id')

    if external_id is not None:
        query = query.join(Patient).filter(Patient.external_id == external_id)

    payments_models = query.all()
    payments_models = [payment.render() for payment in payments_models]

    return make_response(jsonify(payments_models))


def payments_post():
    answer = payments_load(request.json)
    return make_response(jsonify(answer))


def payments_load(payload):
    session = Session()
    models = []

    for raw_payment in payload:
        patient_id = raw_payment['patientId']
        patient = session.query(Patient).filter_by(external_id=patient_id).first()
        if patient is None:
            # This patient was deleted so payment will deleted in internal system or is not synced
            continue
        raw_payment['patient_id'] = patient.id
        model_id = upsert_model(raw_payment, Payment)
        models.append(model_id)

    delete_models(models, Payment)
    return {'result': True}


def patients_post():
    answer = patients_load(request.json)
    return make_response(jsonify(answer))


def patients_load(payload):
    models = []
    for raw_patient in payload:
        raw_patient['dateOfBirth'] = to_date_format(raw_patient['dateOfBirth'])
        model_id = upsert_model(raw_patient, Patient)
        models.append(model_id)
    delete_models(models, Patient)
    return {'result': True}


def patients_get():
    session = Session()

    query = (session.query(Patient.id,
                           Patient.first_name,
                           Patient.last_name,
                           func.sum(Payment.amount))
             .join(Payment)
             .group_by(Patient.id))

    try:
        payment_min = float(request.args.get('payment_min'))
        query = query.having(func.sum(Payment.amount) >= payment_min)
    except (ValueError, TypeError):
        pass

    try:
        payment_max = float(request.args.get('payment_max'))
        query = query.having(func.sum(Payment.amount) <= payment_max)
    except (ValueError, TypeError):
        pass

    patients_raw = query.all()

    answer = [dict(id=patient[0],
                   first_name=patient[1],
                   last_name=patient[2],
                   sum=patient[3]) for patient in patients_raw]

    return make_response(jsonify({'result': answer}))


api_patients = Blueprint('api_patients', __name__)
api_payments = Blueprint('api_payments', __name__)


@api_patients.route('/patients', methods=['POST', 'GET'])
def patients():
    method = patients_post if request.method == 'POST' else patients_get
    return method()


@api_payments.route('/payments', methods=['POST', 'GET'])
def payments():
    method = payments_post if request.method == 'POST' else payments_get
    return method()
