'''
drivenow_orm.py

DriveNow – Vehicle Management System (SQLAlchemy ORM + RabbitMQ)

This module implements a layered CLI application for managing:
- Cars (cars table)
- Rentals (rentals table)

It uses:
- SQLAlchemy ORM for persistence
- PostgreSQL (via psycopg2 driver)
- Prometheus metrics exporter
- Python logging to console + file
- RabbitMQ for event-based communication

Project layout (recommended)
---------------------------
C://CV//Ness_Assignment//
  drivenow_orm.py
  mq.py
  worker.py
  Utility//
    database.ini
  tests//
    ...

database.ini format
-------------------
[postgresql]
host=localhost
database=Carpool
user=java
password=script
port=5432

CLI commands
------------
Initialize/create tables:
  python drivenow_orm.py init-db

Reset (DROP + recreate) tables:
  python drivenow_orm.py reset-db

Add a car:
  python drivenow_orm.py add-car --model "Mazda 3" --year 2020 --status available

Update a car:
  python drivenow_orm.py update-car --car-id 1 --status under_maintenance

Delete a car:
  python drivenow_orm.py delete-car --car-id 1

List cars:
  python drivenow_orm.py list-cars
  python drivenow_orm.py list-cars --status in_use

Start a rental:
  python drivenow_orm.py start-rental --car-id 1 --customer "Alice" --start 2026-02-27

End a rental:
  python drivenow_orm.py end-rental --rental-id 1 --end 2026-03-01

Run metrics server:
  python drivenow_orm.py metrics --port 8000

RabbitMQ event communication
----------------------------
The Service layer publishes domain events after successful operations:
- CarAdded
- CarUpdated
- CarDeleted
- RentalStarted
- RentalEnded

Those events are consumed by worker.py through RabbitMQ.

Architecture
------------
CLI (argparse) -> Service (business rules + publish MQ events)
                -> Repository (SQLAlchemy DB access)
                -> PostgreSQL

Worker (RabbitMQ consumer) -> receives events asynchronously
'''

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime
from enum import Enum
from typing import Optional, List, Dict

from configparser import ConfigParser

from sqlalchemy import (
    create_engine,
    String,
    Integer,
    Date,
    ForeignKey,
    CheckConstraint,
    select,
    func,
    text,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
    sessionmaker,
    Session,
)
from sqlalchemy.exc import IntegrityError
from prometheus_client import Gauge, Histogram, start_http_server

from mq import EventPublisher


# ----------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------

def setup_logging(log_file: str = "drivenow.log", level: str = "INFO") -> None:
    """
    Configure logging to both console and file.

    Console uses the requested level.
    File always logs DEBUG and above.
    """
    logger = logging.getLogger("drivenow")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    logger.handlers.clear()

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, level.upper(), logging.INFO))
    console_handler.setFormatter(fmt)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)


log = logging.getLogger("drivenow")


# ----------------------------------------------------------------------
# Prometheus metrics
# ----------------------------------------------------------------------

CARS_TOTAL = Gauge("drivenow_cars_total", "Total number of cars")
CARS_AVAILABLE = Gauge("drivenow_cars_available", "Number of available cars")
CARS_IN_USE = Gauge("drivenow_cars_in_use", "Number of cars currently in use")
CARS_MAINTENANCE = Gauge("drivenow_cars_under_maintenance", "Number of cars under maintenance")
RENTALS_ONGOING = Gauge("drivenow_rentals_ongoing", "Number of ongoing rentals (end_date is NULL)")
OP_DURATION = Histogram(
    "drivenow_operation_duration_seconds",
    "Operation duration in seconds",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10),
)


def timed(op_name: str):
    """
    Decorator to measure operation duration and record it in Prometheus.
    """
    def deco(fn):
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                return fn(*args, **kwargs)
            finally:
                dur = time.perf_counter() - start
                OP_DURATION.observe(dur)
                log.debug("op=%s duration=%.4fs", op_name, dur)
        return wrapper
    return deco


# ----------------------------------------------------------------------
# Configuration (database.ini -> SQLAlchemy URL)
# ----------------------------------------------------------------------

def read_db_ini(section: str = "postgresql") -> Dict[str, str]:
    """
    Read Utility/database.ini relative to this script file.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    ini_path = os.path.join(base_dir, "Utility", "database.ini")

    parser = ConfigParser()
    parser.read(ini_path)

    if not parser.has_section(section):
        raise FileNotFoundError(f"Missing [{section}] in {ini_path}")

    return {k: v for k, v in parser.items(section)}


def make_sqlalchemy_url(params: Dict[str, str]) -> str:
    """
    Convert database.ini params into a SQLAlchemy connection URL.
    """
    user = params.get("user", "")
    password = params.get("password", "")
    host = params.get("host", "localhost")
    port = params.get("port", "5432")
    db = params.get("database", "")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


# ----------------------------------------------------------------------
# Domain helpers
# ----------------------------------------------------------------------

class CarStatus(str, Enum):
    """
    Allowed statuses for cars.
    """
    AVAILABLE = "available"
    IN_USE = "in_use"
    UNDER_MAINTENANCE = "under_maintenance"

    @staticmethod
    def parse(value: str) -> "CarStatus":
        """
        Parse a user-provided status string into a CarStatus enum.
        """
        v = (value or "").strip().lower()
        mapping = {
            "available": CarStatus.AVAILABLE,
            "in_use": CarStatus.IN_USE,
            "inuse": CarStatus.IN_USE,
            "in use": CarStatus.IN_USE,
            "under_maintenance": CarStatus.UNDER_MAINTENANCE,
            "maintenance": CarStatus.UNDER_MAINTENANCE,
            "under maintenance": CarStatus.UNDER_MAINTENANCE,
        }
        if v not in mapping:
            raise ValueError("Invalid status. Use: available | in_use | under_maintenance")
        return mapping[v]


def parse_iso_date(s: str) -> date:
    """
    Parse YYYY-MM-DD into a date.
    """
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception as e:
        raise ValueError(f"Invalid date '{s}'. Expected YYYY-MM-DD.") from e


# ----------------------------------------------------------------------
# SQLAlchemy ORM models
# ----------------------------------------------------------------------

class Base(DeclarativeBase):
    """Base class for ORM models."""
    pass


class Car(Base):
    """
    ORM model for the cars table.
    """
    __tablename__ = "cars"

    car_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    model: Mapped[str] = mapped_column(String, nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)

    rentals: Mapped[List["Rental"]] = relationship(
        back_populates="car",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    __table_args__ = (
        CheckConstraint("year >= 1886 AND year <= 3000", name="cars_year_range"),
        CheckConstraint(
            "status IN ('available','in_use','under_maintenance')",
            name="cars_status_allowed",
        ),
    )


class Rental(Base):
    """
    ORM model for the rentals table.
    """
    __tablename__ = "rentals"

    rental_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    car_id: Mapped[int] = mapped_column(Integer, ForeignKey("cars.car_id", ondelete="CASCADE"), nullable=False)
    customer_name: Mapped[str] = mapped_column(String, nullable=False)
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    end_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)

    car: Mapped[Car] = relationship(back_populates="rentals")

    __table_args__ = (
        CheckConstraint("(end_date IS NULL) OR (end_date >= start_date)", name="rentals_end_after_start"),
    )


# ----------------------------------------------------------------------
# Repository layer
# ----------------------------------------------------------------------

class DriveNowRepository:
    """
    Repository layer: handles all DB access through SQLAlchemy sessions.
    """

    def __init__(self, session_factory: sessionmaker[Session]):
        self.SessionFactory = session_factory

    @timed("db.create_tables")
    def create_tables(self) -> None:
        """
        Create cars then rentals if they don't already exist.
        """
        engine = self.SessionFactory.kw["bind"]
        Base.metadata.create_all(engine, tables=[Car.__table__], checkfirst=True)
        Base.metadata.create_all(engine, tables=[Rental.__table__], checkfirst=True)

    @timed("db.reset_tables")
    def reset_tables(self) -> None:
        """
        Drop rentals and cars, then recreate them.
        """
        engine = self.SessionFactory.kw["bind"]
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS rentals CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS cars CASCADE"))
        self.create_tables()

    @timed("cars.add")
    def add_car(self, model: str, year: int, status: CarStatus) -> int:
        """
        Insert a new car and return its generated car_id.
        """
        with self.SessionFactory() as s:
            car = Car(model=model, year=year, status=status.value)
            s.add(car)
            s.commit()
            s.refresh(car)
            return car.car_id

    @timed("cars.update")
    def update_car(self, car_id: int, model: Optional[str], year: Optional[int], status: Optional[CarStatus]) -> int:
        """
        Update an existing car. Returns 1 if updated, 0 if not found.
        """
        with self.SessionFactory() as s:
            car = s.get(Car, car_id)
            if car is None:
                return 0
            if model is not None:
                car.model = model
            if year is not None:
                car.year = year
            if status is not None:
                car.status = status.value
            s.commit()
            return 1

    @timed("cars.delete")
    def delete_car(self, car_id: int) -> int:
        """
        Delete a car by id. Returns 1 if deleted, 0 if not found.
        """
        with self.SessionFactory() as s:
            car = s.get(Car, car_id)
            if car is None:
                return 0
            s.delete(car)
            s.commit()
            return 1

    @timed("cars.get")
    def get_car(self, car_id: int) -> Optional[Car]:
        """
        Fetch a car by id.
        """
        with self.SessionFactory() as s:
            return s.get(Car, car_id)

    @timed("cars.list")
    def list_cars(self, status: Optional[CarStatus]) -> List[Car]:
        """
        List all cars, optionally filtered by status.
        """
        with self.SessionFactory() as s:
            stmt = select(Car).order_by(Car.car_id)
            if status is not None:
                stmt = stmt.where(Car.status == status.value)
            return list(s.scalars(stmt).all())

    @timed("rentals.start")
    def start_rental(self, car_id: int, customer: str, start: date) -> int:
        """
        Insert a new rental and return its rental_id.
        """
        with self.SessionFactory() as s:
            rental = Rental(car_id=car_id, customer_name=customer, start_date=start, end_date=None)
            s.add(rental)
            s.commit()
            s.refresh(rental)
            return rental.rental_id

    @timed("rentals.get")
    def get_rental(self, rental_id: int) -> Optional[Rental]:
        """
        Fetch a rental by id.
        """
        with self.SessionFactory() as s:
            return s.get(Rental, rental_id)

    @timed("rentals.end")
    def end_rental(self, rental_id: int, end: date) -> int:
        """
        End an ongoing rental. Returns 1 if updated, 0 otherwise.
        """
        with self.SessionFactory() as s:
            rental = s.get(Rental, rental_id)
            if rental is None or rental.end_date is not None:
                return 0
            rental.end_date = end
            s.commit()
            return 1

    @timed("metrics.refresh")
    def refresh_metrics(self) -> None:
        """
        Refresh Prometheus gauge metrics from current DB state.
        """
        with self.SessionFactory() as s:
            total = s.scalar(select(func.count()).select_from(Car)) or 0
            by_status = dict(s.execute(select(Car.status, func.count()).group_by(Car.status)).all())
            ongoing = s.scalar(select(func.count()).select_from(Rental).where(Rental.end_date.is_(None))) or 0

        CARS_TOTAL.set(int(total))
        CARS_AVAILABLE.set(int(by_status.get("available", 0)))
        CARS_IN_USE.set(int(by_status.get("in_use", 0)))
        CARS_MAINTENANCE.set(int(by_status.get("under_maintenance", 0)))
        RENTALS_ONGOING.set(int(ongoing))


# ----------------------------------------------------------------------
# Service layer
# ----------------------------------------------------------------------

class DriveNowService:
    """
    Service layer: enforces business rules and publishes MQ events after success.
    """

    def __init__(self, repo: DriveNowRepository, publisher: Optional[EventPublisher] = None):
        self.repo = repo
        self.publisher = publisher

    @timed("service.add_car")
    def add_car(self, model: str, year: int, status: CarStatus) -> int:
        """
        Validate and create a car, then publish CarAdded event.
        """
        if not model or not model.strip():
            raise ValueError("model is required")
        if year < 1886 or year > 3000:
            raise ValueError("year out of range")

        model_clean = model.strip()
        car_id = self.repo.add_car(model_clean, year, status)
        log.info("Added car car_id=%s model=%s year=%s status=%s", car_id, model_clean, year, status.value)
        self.repo.refresh_metrics()

        print("DEBUG: publishing CarAdded event")
        
        if self.publisher:
            self.publisher.publish(
                "CarAdded",
                {"car_id": car_id, "model": model_clean, "year": year, "status": status.value},
                routing_key="drivenow.car.added",
            )

        return car_id

    @timed("service.update_car")
    def update_car(self, car_id: int, model: Optional[str], year: Optional[int], status: Optional[CarStatus]) -> None:
        """
        Validate and update a car, then publish CarUpdated event.
        """
        if year is not None and (year < 1886 or year > 3000):
            raise ValueError("year out of range")

        model_clean = model.strip() if model else None
        affected = self.repo.update_car(car_id, model_clean, year, status)
        if affected == 0:
            raise ValueError(f"No car updated (car_id={car_id}).")

        log.info("Updated car car_id=%s", car_id)
        self.repo.refresh_metrics()

        if self.publisher:
            self.publisher.publish(
                "CarUpdated",
                {
                    "car_id": car_id,
                    "model": model_clean,
                    "year": year,
                    "status": status.value if status else None,
                },
                routing_key="drivenow.car.updated",
            )

    @timed("service.delete_car")
    def delete_car(self, car_id: int) -> None:
        """
        Delete a car, then publish CarDeleted event.
        """
        affected = self.repo.delete_car(car_id)
        if affected == 0:
            raise ValueError(f"Car not found: car_id={car_id}")

        log.info("Deleted car car_id=%s", car_id)
        self.repo.refresh_metrics()

        if self.publisher:
            self.publisher.publish(
                "CarDeleted",
                {"car_id": car_id},
                routing_key="drivenow.car.deleted",
            )

    @timed("service.list_cars")
    def list_cars(self, status: Optional[CarStatus]) -> List[Car]:
        """
        List cars and refresh metrics.
        """
        cars = self.repo.list_cars(status)
        self.repo.refresh_metrics()
        return cars

    @timed("service.start_rental")
    def start_rental(self, car_id: int, customer: str, start: date) -> int:
        """
        Start a rental for an available car, then publish RentalStarted event.
        """
        car = self.repo.get_car(car_id)
        if car is None:
            raise ValueError(f"Car not found: car_id={car_id}")
        if car.status != CarStatus.AVAILABLE.value:
            raise ValueError(f"Car is not available (status={car.status}).")

        customer_clean = customer.strip()
        self.repo.update_car(car_id, None, None, CarStatus.IN_USE)
        rid = self.repo.start_rental(car_id, customer_clean, start)

        log.info("Started rental rental_id=%s car_id=%s customer=%s", rid, car_id, customer_clean)
        self.repo.refresh_metrics()

        print("DEBUG: publishing RentalStarted event")

        if self.publisher:
            self.publisher.publish(
                "RentalStarted",
                {
                    "rental_id": rid,
                    "car_id": car_id,
                    "customer": customer_clean,
                    "start_date": start.isoformat(),
                },
                routing_key="drivenow.rental.started",
            )

        return rid

    @timed("service.end_rental")
    def end_rental(self, rental_id: int, end: date) -> None:
        """
        End an ongoing rental, restore car availability, then publish RentalEnded event.
        """
        rental = self.repo.get_rental(rental_id)
        if rental is None:
            raise ValueError(f"Rental not found: rental_id={rental_id}")
        if rental.end_date is not None:
            raise ValueError(f"Rental already ended (end_date={rental.end_date}).")
        if end < rental.start_date:
            raise ValueError("end date cannot be before start date")

        affected = self.repo.end_rental(rental_id, end)
        print(f"DEBUG: repo.end_rental affected={affected}")
        
        if affected == 0:
            raise ValueError("Could not end rental (maybe already ended).")

        self.repo.update_car(rental.car_id, None, None, CarStatus.AVAILABLE)
        log.info("Ended rental rental_id=%s car_id=%s", rental_id, rental.car_id)
        self.repo.refresh_metrics()

        print("DEBUG: publishing RentalEnded event")

        if self.publisher:
            self.publisher.publish(
                "RentalEnded",
                {
                    "rental_id": rental_id,
                    "car_id": rental.car_id,
                    "end_date": end.isoformat(),
                },
                routing_key="drivenow.rental.ended",
            )


# ----------------------------------------------------------------------
# CLI helpers
# ----------------------------------------------------------------------

def print_cars(cars: List[Car]) -> None:
    """
    Pretty-print a list of cars.
    """
    if not cars:
        print("(no cars)")
        return
    print("car_id | model | year | status")
    print("-" * 60)
    for c in cars:
        print(f"{c.car_id} | {c.model} | {c.year} | {c.status}")


def build_parser() -> argparse.ArgumentParser:
    """
    Build the CLI parser and subcommands, including architecture help text.
    """
    architecture = (
        "\nArchitecture (text diagram)\n"
        "---------------------------\n"
        "CLI (argparse) -> Service (business rules + MQ events) -> Repository (SQLAlchemy) -> PostgreSQL\n"
        "       |                    |\n"
        "       |                    +--> RabbitMQ publisher (domain events)\n"
        "       +--> User commands\n"
        "\n"
        "Worker (worker.py) -> RabbitMQ consumer -> asynchronous event handling\n"
    )

    p = argparse.ArgumentParser(
        prog="drivenow-orm",
        description=(
            "DriveNow ORM CLI (SQLAlchemy + RabbitMQ)\n"
            "Manage cars and rentals with layered architecture and MQ-based communication."
        ),
        epilog=architecture,
        formatter_class=argparse.RawTextHelpFormatter,
    )

    p.add_argument("--log-file", default="drivenow.log", help="Log file path (default: drivenow.log)")
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Console log level",
    )

    sub = p.add_subparsers(dest="cmd", required=True, metavar="command")

    sub.add_parser(
        "init-db",
        help="Create tables if missing",
        description=(
            "init-db\n"
            "-------\n"
            "Create the required tables if they do not already exist."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )

    sub.add_parser(
        "reset-db",
        help="Drop and recreate tables (cars, rentals)",
        description=(
            "reset-db\n"
            "--------\n"
            "Drop rentals and cars, then recreate them from scratch."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )

    add = sub.add_parser(
        "add-car",
        help="Add a new car",
        description=(
            "add-car\n"
            "-------\n"
            "Add a new car to the database and publish CarAdded."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    add.add_argument("--model", required=True, help="Car model")
    add.add_argument("--year", required=True, type=int, help="Car year")
    add.add_argument(
        "--status",
        default="available",
        choices=["available", "in_use", "under_maintenance"],
        help="Initial status",
    )

    upd = sub.add_parser(
        "update-car",
        help="Update a car",
        description=(
            "update-car\n"
            "----------\n"
            "Update an existing car and publish CarUpdated."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    upd.add_argument("--car-id", required=True, type=int, help="ID of the car to update")
    upd.add_argument("--model", help="New model")
    upd.add_argument("--year", type=int, help="New year")
    upd.add_argument("--status", choices=["available", "in_use", "under_maintenance"], help="New status")

    dele = sub.add_parser(
        "delete-car",
        help="Delete a car by id (also deletes its rentals)",
        description=(
            "delete-car\n"
            "----------\n"
            "Delete a car and publish CarDeleted."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    dele.add_argument("--car-id", required=True, type=int, help="ID of the car to delete")

    lst = sub.add_parser(
        "list-cars",
        help="List cars",
        description=(
            "list-cars\n"
            "--------\n"
            "List all cars, optionally filtered by status."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    lst.add_argument("--status", choices=["available", "in_use", "under_maintenance"], help="Optional status filter")

    sr = sub.add_parser(
        "start-rental",
        help="Start a rental",
        description=(
            "start-rental\n"
            "-----------\n"
            "Start a rental for an available car and publish RentalStarted."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    sr.add_argument("--car-id", required=True, type=int, help="Car ID")
    sr.add_argument("--customer", required=True, help="Customer name")
    sr.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")

    er = sub.add_parser(
        "end-rental",
        help="End a rental",
        description=(
            "end-rental\n"
            "---------\n"
            "End an ongoing rental and publish RentalEnded."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    er.add_argument("--rental-id", required=True, type=int, help="Rental ID")
    er.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")

    met = sub.add_parser(
        "metrics",
        help="Run Prometheus metrics server",
        description=(
            "metrics\n"
            "------\n"
            "Expose Prometheus metrics at /metrics and refresh gauges every 2 seconds."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    met.add_argument("--port", type=int, default=8000, help="Metrics port (default: 8000)")

    return p


# ----------------------------------------------------------------------
# Main entry point
# ----------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    """
    CLI entry point.
    """
    parser = build_parser()
    args = parser.parse_args(argv)

    setup_logging(args.log_file, args.log_level)

    params = read_db_ini()
    url = make_sqlalchemy_url(params)
    engine = create_engine(url, pool_pre_ping=True)
    SessionFactory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)

    repo = DriveNowRepository(SessionFactory)

    mq_url = os.getenv("DRIVENOW_RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
    publisher = EventPublisher(url=mq_url)
    svc = DriveNowService(repo, publisher=publisher)

    try:
        if args.cmd == "init-db":
            repo.create_tables()
            repo.refresh_metrics()
            print("OK: tables ensured (cars, rentals).")

        elif args.cmd == "reset-db":
            repo.reset_tables()
            repo.refresh_metrics()
            print("OK: tables dropped and recreated (cars, rentals).")

        elif args.cmd == "add-car":
            car_id = svc.add_car(args.model, args.year, CarStatus.parse(args.status))
            print(f"OK: added car_id={car_id}")

        elif args.cmd == "update-car":
            status = CarStatus.parse(args.status) if args.status else None
            svc.update_car(args.car_id, args.model, args.year, status)
            print("OK: updated.")

        elif args.cmd == "delete-car":
            svc.delete_car(args.car_id)
            print("OK: deleted.")

        elif args.cmd == "list-cars":
            status = CarStatus.parse(args.status) if args.status else None
            cars = svc.list_cars(status)
            print_cars(cars)

        elif args.cmd == "start-rental":
            rid = svc.start_rental(args.car_id, args.customer, parse_iso_date(args.start))
            print(f"OK: started rental_id={rid}")

        elif args.cmd == "end-rental":
            svc.end_rental(args.rental_id, parse_iso_date(args.end))
            print("OK: ended rental.")

        elif args.cmd == "metrics":
            port = int(args.port)
            start_http_server(port)
            print(f"Metrics: http://localhost:{port}/metrics")
            log.info("Metrics server running on port %s", port)

            while True:
                try:
                    repo.refresh_metrics()
                except Exception:
                    log.exception("Failed refreshing metrics")
                time.sleep(2)

        else:
            parser.print_help()
            return 2

        return 0

    except IntegrityError as e:
        log.exception("DB IntegrityError")
        print(f"DB ERROR: {e.orig}")
        return 1
    except Exception as e:
        log.exception("Error")
        print(f"ERROR: {e}")
        return 1
    finally:
        try:
            engine.dispose()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())