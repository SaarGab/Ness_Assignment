Architecture Overview:

The project follows a layered architecture to keep responsibilities separated and the code easy to maintain.

                +------------------+
                |      CLI         |
                |   (argparse)     |
                +---------+--------+
                          |
                          v
                +------------------+
                |     Service      |
                | Business Rules   |
                | + Publish Events |
                +---------+--------+
                          |
                          v
                +------------------+
                |   Repository     |
                | SQLAlchemy ORM   |
                +---------+--------+
                          |
                          v
                +------------------+
                |    PostgreSQL    |
                |    Database      |
                +------------------+

                 RabbitMQ Message Queue
                          |
                          v
                +------------------+
                |     Worker       |
                | Event Consumer   |
                | (worker.py)      |
                +------------------+

Responsibilities:

CLI -

Parses user commands

Displays results

Calls the Service layer

Service Layer -

Implements business rules

Coordinates repository operations

Publishes domain events to RabbitMQ

Repository Layer -

Handles all database interactions

Uses SQLAlchemy ORM

Worker -

Consumes RabbitMQ events asynchronously

Handles event processing (logging/auditing)

Technologies Used:

Python 3.11+

SQLAlchemy ORM

PostgreSQL

RabbitMQ

Prometheus metrics

Docker + Docker Compose

Project Structure:

drivenow/

│

├── drivenow_orm.py      # Main application (CLI + Service + Repository)

├── mq.py                # RabbitMQ publisher

├── worker.py            # RabbitMQ consumer

│

├── docker-compose.yaml  # Runs DB, RabbitMQ, worker

├── requirements.txt     # Python dependencies

│

├── Utility/

│   └── database.ini     # Database configuration

│

├── tests/               # Unit tests

│

└── README.md

How to Run the Project:

1. Install dependencies
pip install -r requirements.txt
2. Start infrastructure with Docker

Start PostgreSQL, RabbitMQ, and the worker:

docker compose up -d

RabbitMQ UI will be available at:

http://localhost:15672
username: guest
password: guest
3. Initialize the database
python drivenow_orm.py init-db

Or reset it:

python drivenow_orm.py reset-db
Using the CLI

The application is controlled through CLI commands.

Show all commands:

python drivenow_orm.py --help

Example output:

command:
  init-db
  reset-db
  add-car
  update-car
  delete-car
  list-cars
  start-rental
  end-rental
  metrics
CLI Commands
Add a Car
python drivenow_orm.py add-car --model "Mazda 3" --year 2020

Optional status:

--status available
--status in_use
--status under_maintenance
Update a Car
python drivenow_orm.py update-car --car-id 1 --status under_maintenance
Delete a Car
python drivenow_orm.py delete-car --car-id 1

Deleting a car automatically deletes its rentals due to ON DELETE CASCADE.

List Cars

List all cars:

python drivenow_orm.py list-cars

Filter by status:

python drivenow_orm.py list-cars --status available
Start a Rental
python drivenow_orm.py start-rental \
  --car-id 1 \
  --customer "Alice" \
  --start 2026-03-01

Rules:

Car must exist

Car must be available

End a Rental
python drivenow_orm.py end-rental \
  --rental-id 1 \
  --end 2026-03-05

Rules:

Rental must exist

Rental must not already be ended

End date must be ≥ start date

Message Queue Communication

The system uses RabbitMQ to enable asynchronous communication between components.

After each successful operation, the service publishes events:

Event	Description
CarAdded	A new car was added
CarUpdated	Car information updated
CarDeleted	Car removed from system
RentalStarted	A rental began
RentalEnded	A rental ended

Example event message:

{
  "type": "CarAdded",
  "payload": {
    "car_id": 1,
    "model": "Mazda 3",
    "year": 2020,
    "status": "available"
  }
}

These events are consumed by worker.py.

Running the Worker

Start the worker:

python worker.py

The worker will print received events:

[WORKER] received event: type=CarAdded payload={...}
Metrics (Prometheus)

Start the metrics server:

python drivenow_orm.py metrics

Open:

http://localhost:8000/metrics

Available metrics include:

drivenow_cars_total

drivenow_cars_available

drivenow_cars_in_use

drivenow_rentals_ongoing

drivenow_operation_duration_seconds

Example Workflow

Example session:

python drivenow_orm.py init-db

python drivenow_orm.py add-car --model "Mazda 3" --year 2020

python drivenow_orm.py list-cars

python drivenow_orm.py start-rental \
    --car-id 1 \
    --customer "Alice" \
    --start 2026-03-01

python drivenow_orm.py end-rental \
    --rental-id 1 \
    --end 2026-03-05

Meanwhile the worker receives events like:

CarAdded
RentalStarted
RentalEnded
Design Goals

The system was designed to demonstrate:

Clean separation of concerns

Maintainable layered architecture

Database persistence with SQLAlchemy

Asynchronous communication via RabbitMQ

Observability with Prometheus metrics

