from langchain_astradb import AstraDBVectorStore
from langchain_huggingface import HuggingFaceEndpointEmbeddings
from flipkart.data_converter import DataConverter
from flipkart.config import Config

class DataIngestor:
    def __init__(self):
        self.embeddings = HuggingFaceEndpointEmbeddings(model=Config.EMBEDDING_MODEL)
        self.vector_store = AstraDBVectorStore(embedding=self.embeddings, collection_name="flipkartdb", api_endpoint=Config.ASTRA_DB_API_ENDPOINT, token=Config.ASTRA_DB_APPLICATION_TOKEN, namespace=Config.ASTRA_DB_KEYSPACE)

    def ingest(self, load_existing=True):
        if load_existing == True:
            return self.vector_store
        
        converter = DataConverter("data/flipkart_product_review.csv")
        docs = converter.convert()

        self.vector_store.add_documents(docs)
        return self.vector_store
    
ingester = DataIngestor()
ingester.ingest(load_existing=False)