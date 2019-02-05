import datetime

import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy.engine import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, backref
from sqlalchemy.schema import Index


SQLALCHEMY_DATABASE_URI = 'postgresql://flask_test:flask_test@localhost:5432/flask_test'
engine = create_engine(SQLALCHEMY_DATABASE_URI, echo=True)
Session = sessionmaker(bind=engine)


def to_date_format(raw_date):
    if isinstance(raw_date, str):
        return datetime.datetime.strptime(raw_date, '%Y-%M-%d').date()
    return raw_date


class Base(object):
    __abstract__ = True
    id = sa.Column(sa.Integer, primary_key=True)
    # TODO: move to declared_attr
    created = sa.Column(sa.DateTime)
    updated = sa.Column(sa.DateTime)

    _MAPPER = None

    def render(self):
        return {'created': self.created.isoformat(), 'updated': self.updated.isoformat()}

    def set_updated(self):
        self.updated = datetime.datetime.now().date()

    def set_created(self):
        self.created = datetime.datetime.now().date()

    @classmethod
    def create(cls, raw_data):
        raise NotImplementedError

    def update(self, raw_data):
        for model_field, raw_field in self._MAPPER.items():
            if raw_data.get(raw_field) is not None:
                setattr(self, model_field, raw_data[raw_field])


Base = declarative_base(cls=Base)


class Patient(Base):
    __tablename__ = 'patients'

    id = sa.Column(sa.Integer, primary_key=True)
    first_name = sa.Column(sa.String, nullable=False)
    last_name = sa.Column(sa.String, nullable=False)
    middle_name = sa.Column(sa.String)
    date_of_birth = sa.Column(sa.Date)
    external_id = sa.Column(sa.String)

    payment = relationship('Payment',
                           backref=backref('patients', cascade="all,delete"),
                           passive_deletes=True)

    __table_args__ = (
        Index('pk_patients_external_id', external_id, unique=True),
    )
    _MAPPER = dict(first_name='firstName',
                   last_name='lastName',
                   middle_name='middleName',
                   date_of_birth='dateOfBirth')

    def render(self):
        return dict(id=self.id,
                    first_name=self.first_name,
                    last_name=self.last_name,
                    middle_name=self.middle_name,
                    date_of_birth=self.date_of_birth.isoformat(),
                    external_id=self.external_id)

    @classmethod
    def create(cls, raw_data):
        model = cls(first_name=raw_data.get('firstName'),
                    last_name=raw_data.get('lastName'),
                    middle_name=raw_data.get('middleName'),
                    external_id=raw_data['externalId'],
                    date_of_birth=to_date_format(raw_data.get('dateOfBirth')))
        return model


class Payment(Base):
    __tablename__ = 'payments'

    id = sa.Column(sa.Integer, primary_key=True)
    amount = sa.Column(sa.Float, nullable=False)
    patient_id = sa.Column(sa.Integer,
                           sa.ForeignKey('patients.id', ondelete='CASCADE'),
                           nullable=False)
    external_id = sa.Column(sa.String)

    __table_args__ = (
        Index('pk_payments_external_id', external_id, unique=True),
    )

    _MAPPER = dict(amount='amount', patient_id='patient_id')

    def render(self):
        return dict(id=self.id,
                    amount=self.amount,
                    patient_id=self.patient_id,
                    external_id=self.external_id)

    @classmethod
    def create(cls, raw_data):
        model = cls(amount=raw_data['amount'],
                    patient_id=raw_data['patient_id'],
                    external_id=raw_data['externalId'])
        return model


@event.listens_for(Patient, 'before_insert')
def before_insert(mapper, connection, target):
    before_insert_created(mapper, connection, target)


@event.listens_for(Payment, 'before_insert')
def before_insert(mapper, connection, target):
    before_insert_created(mapper, connection, target)


@event.listens_for(Patient, 'before_update')
def before_update(mapper, connection, target):
    before_insert_updated(mapper, connection, target)


@event.listens_for(Payment, 'before_update')
def before_update(mapper, connection, target):
    before_insert_updated(mapper, connection, target)


def before_insert_created(mapper, connection, target):
    now = datetime.datetime.utcnow()
    target.updated, target.created = now, now


def before_insert_updated(mapper, connection, target):
    # TODO: add validation changed external_id
    target.updated = datetime.datetime.utcnow()
