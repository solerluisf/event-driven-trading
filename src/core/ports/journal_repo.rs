// journal_repo.rs

pub trait IJournalRepo {
    fn persist_outbound(&self, record: RequestRecord);
    fn persist_inbound(&self, record: ResponseRecord);
    fn replay(&self, query: String) -> Vec<ResponseRecord>;
}