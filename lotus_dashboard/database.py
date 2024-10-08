from sqlalchemy.orm import declarative_base


class Base:

    def to_dict(self):
        return {field.name: getattr(self, field.name) for field in self.__table__.c}

    def from_dict(self, data: dict):
        for field in self.__table__.c:
            if field.name in data:
                setattr(self, field.name, data[field.name])


Base = declarative_base(cls=Base)
