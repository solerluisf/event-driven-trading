// journal_storage.rs

pub struct JournalStorage;

impl IJournalRepo for JournalStorage {
    fn persist_outbound(&self, _record: RequestRecord) {}
    fn persist_inbound(&self, _record: ResponseRecord) {}
    fn replay(&self, _query: String) -> Vec<ResponseRecord> {
        vec![]
    }
}