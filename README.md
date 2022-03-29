# prefect-great-expectations-demo

Project demonstrating how to use Prefect and Great Expectations together with Prefect's `RunGreatExpectationsValidation` task.

Requires Python > 3.7 

To run a simple validation flow against the example project, run the following:

```bash
# Install requirements
pip install -r requirements.txt

# Run the simple validation flow
python 1_simple_validation_flow.py
```

Other flows in this project a designed to be run in Prefect Cloud. Refer to the Prefect documentation for how to [deploy a flow to Prefect Cloud](https://docs.prefect.io/orchestration/getting-started/registering-and-running-a-flow.html).