describe Cadence::Connection::Thrift do
  subject { described_class.new(nil, nil, nil) }
  let(:thrift_stub) { double('CadenceThrift::WorkflowService::Client') }
  let(:domain) { 'test-domain' }
  let(:workflow_id) { SecureRandom.uuid }
  let(:run_id) { SecureRandom.uuid }

  before do
    allow(subject).to receive(:connection).and_return(thrift_stub)
  end

  describe '#get_workflow_execution_history' do
    let(:response) do
      CadenceThrift::GetWorkflowExecutionHistoryResponse.new(
        history: CadenceThrift::History.new,
        nextPageToken: nil
      )
    end

    before do
      allow(thrift_stub)
        .to receive(:GetWorkflowExecutionHistory)
        .with(an_instance_of(CadenceThrift::GetWorkflowExecutionHistoryRequest))
        .and_return(response)
    end

    it 'calls Thrift service with supplied arguments' do
      subject.get_workflow_execution_history(
        domain: domain,
        workflow_id: workflow_id,
        run_id: run_id
      )

      expect(thrift_stub).to have_received(:GetWorkflowExecutionHistory) do |request|
        expect(request).to be_an_instance_of(CadenceThrift::GetWorkflowExecutionHistoryRequest)
        expect(request.domain).to eq(domain)
        expect(request.execution.workflowId).to eq(workflow_id)
        expect(request.execution.runId).to eq(run_id)
        expect(request.nextPageToken).to be_nil
        expect(request.waitForNewEvent).to eq(false)
        expect(request.HistoryEventFilterType).to eq(
          CadenceThrift::HistoryEventFilterType::ALL_EVENT
        )
      end
    end

    context 'when wait_for_new_event is true' do
      it 'calls Thrift service' do
        subject.get_workflow_execution_history(
          domain: domain,
          workflow_id: workflow_id,
          run_id: run_id,
          wait_for_new_event: true
        )

        expect(thrift_stub).to have_received(:GetWorkflowExecutionHistory) do |request|
          expect(request.waitForNewEvent).to eq(true)
        end
      end
    end

    context 'when event_type is :close' do
      it 'calls Thrift service' do
        subject.get_workflow_execution_history(
          domain: domain,
          workflow_id: workflow_id,
          run_id: run_id,
          event_type: :close
        )

        expect(thrift_stub).to have_received(:GetWorkflowExecutionHistory) do |request|
          expect(request.HistoryEventFilterType).to eq(
            CadenceThrift::HistoryEventFilterType::CLOSE_EVENT
          )
        end
      end
    end
  end
end
