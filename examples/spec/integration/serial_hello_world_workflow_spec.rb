require 'workflows/serial_hello_world_workflow'

describe SerialHelloWorldWorkflow, :integration do
  it 'completes' do
    result = run_workflow(described_class, 'Alice', 'Bob', 'John')

    expect(result.history.events.first.event_type)
      .to eq(CadenceThrift::EventType::WorkflowExecutionCompleted)
  end
end
