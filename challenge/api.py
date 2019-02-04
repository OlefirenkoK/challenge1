import datetime

from flask import request, jsonify, make_response
from flask import Blueprint
from sqlalchemy.exc import DBAPIError

from challenge.models import Session, Payment, Patient
from challenge.run import app


def to_date_format(raw_date):
    return datetime.datetime.strptime(raw_date, '%Y-%M-%d').date()


def payments_get():
    session = Session()

    query = session.query(Payment)
    payment_min = request.args.get('payment_min')
    payment_max = request.args.get('payment_max')
    external_id = request.args.get('external_id')

    # TODO: to decimal
    try:
        payment_min = float(payment_min)
        query = query.filter(Payment.amount >= float(payment_min))
    except (ValueError, TypeError, KeyError):
        pass

    try:
        payment_max = float(payment_max)
        query = query.filter(Payment.amount <= float(payment_max))
    except (ValueError, TypeError, KeyError):
        pass

    if external_id is not None:
        query = query.join(Patient).filter(Patient.external_id == external_id)

    payments_models = query.all()
    payments_models = [payment.render() for payment in payments_models]

    return make_response(jsonify(payments_models))


def payments_post():
    session = Session()
    for raw_payment in request.json:
        patient_id = raw_payment['patientId']
        patient = session.query(Patient).filter_by(external_id=patient_id).first()

        if patient is None:
            return make_response(jsonify({'error': 'Not found: {}'.format(patient_id)}))

        payment = Payment(amount=raw_payment['amount'],
                          patient_id=patient.id,
                          external_id=raw_payment['externalId'])
        session.add(payment)

    session.commit()
    return make_response(jsonify({'result': True}))


api_patients = Blueprint('api_patients', __name__)
api_payments = Blueprint('api_payments', __name__)


@api_patients.route('/patients', methods=['POST'])
def patients():
    session = Session()
    for raw_patient in request.json:
        patient = Patient(first_name=raw_patient.get('firstName'),
                          last_name=raw_patient.get('lastName'),
                          middle_name=raw_patient.get('middleName'),
                          external_id=raw_patient.get('externalId'),
                          date_of_birth=to_date_format(raw_patient.get('dateOfBirth')))
        session.add(patient)
        try:
            session.flush()
        except DBAPIError as exc:
            app.logger.warn(exc)
            return make_response(jsonify({'error': 'Integrity error'}))
        except Exception as exc:
            app.logger.error(exc, exc_info=True)
            return make_response(jsonify({'error': 'Internal error'}))
    session.commit()

    return make_response(jsonify({'result': True}))


@api_payments.route('/payments', methods=['POST', 'GET'])
def payments():
    method = payments_post if request.method == 'POST' else payments_get
    return method()
