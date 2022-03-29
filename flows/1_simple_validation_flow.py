from prefect import Flow
from prefect.tasks.great_expectations import RunGreatExpectationsValidation

# Create a task instance
validation_task = RunGreatExpectationsValidation()

with Flow("Simple Validation") as flow:
    # Run the task within a flow
    validation_task(checkpoint_name="my_checkpoint")

flow.run()