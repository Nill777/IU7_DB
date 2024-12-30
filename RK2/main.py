from sqlalchemy import create_engine, Column, Integer, String, Date, Time, Boolean, ForeignKey, func
from sqlalchemy.orm import declarative_base, sessionmaker

DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/rk1"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()

class Satellite(Base):
    __tablename__ = 'satellite'
    __table_args__ = {'schema': 'rk2'}
    id = Column(Integer, primary_key=True)
    name = Column(String)
    create_data = Column(Date)
    country = Column(String)

class Flight(Base):
    __tablename__ = 'flight'
    __table_args__ = {'schema': 'rk2'}
    satellite_id = Column(Integer, ForeignKey('satellite.id'), primary_key=True)
    flight_date = Column(Date)
    flight_time = Column(Time)
    day = Column(String)
    type = Column(Boolean)

# Самый новый спутник в Китае
newest_satellite_query = session.query(
    Satellite.id,
    Satellite.name,
    Satellite.create_data,
    Satellite.country
).filter(Satellite.country == 'China').order_by(Satellite.create_data.desc()).limit(1)

# Космические аппараты, которые запускались в этом году более двух раз
launches_this_year_query = session.query(
    Flight.satellite_id,
    func.count().label('launch_count')
).filter(func.extract('year', Flight.flight_date) == func.extract('year', func.current_date()), Flight.type == True
         ).group_by(Flight.satellite_id
                    ).having(func.count() > 2)

# Найти все аппараты вернувшиеся на Землю не позднее 10 дней с 2024-01-01
subquery = (
    session.query(Flight.satellite_id)
    .filter(Flight.type == False)
    .filter(Flight.flight_date < '2024-01-11')
).subquery()

returned_ten_days_query = (
    session.query(Flight.satellite_id)
    .filter(Flight.type == True)
    .filter(Flight.flight_date >= '2024-01-01')
    .filter(Flight.satellite_id.in_(subquery))
    .distinct()
)

print("Самый новый спутник в Китае:")
for result in newest_satellite_query.all():
    print(result)

print("\nКосмические аппараты, которые запускались в этом году более двух раз:")
for result in launches_this_year_query.all():
    print(result)

print("\nАппараты, вернувшиеся на Землю не позднее 10 дней с 2024-01-01:")
for result in returned_ten_days_query.all():
    print(result)

session.close()
