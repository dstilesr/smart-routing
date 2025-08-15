from pydantic import Field, BaseModel


class TaskSchema(BaseModel):
    """
    Base Schema for task parameters.
    """

    task_id: str = Field(description="Identifier for the task")
    task_type: str = Field(description="Type of the task to be executed")
    label: str | None = Field(
        default=None, description="Label associated with the task, if any"
    )
    parameters_json: str = Field(
        description="JSON string containing the parameters for the task"
    )
    return_result: bool = Field(
        default=False,
        description="Flag indicating whether to return the result of the task",
    )
