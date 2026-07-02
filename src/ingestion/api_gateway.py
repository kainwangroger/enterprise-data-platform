import os
import json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from kafka import KafkaProducer

app = FastAPI(
    title="EDP Ingestion API Gateway",
    description="Passerelle d'ingestion haut-débit pour les transactions financières d'entreprise.",
    version="1.0.0"
)

# Kafka Broker setup
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_SERVERS", "localhost:9092")
KAFKA_TOPIC = "transactions"

producer = None
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print(f"Liaison avec Kafka réussie sur {KAFKA_BOOTSTRAP_SERVERS}")
except Exception as e:
    print(f"Attention: Impossible de connecter le producteur Kafka ({e}). L'API fonctionnera en mode mock.")

class TransactionPayload(BaseModel):
    transaction_id: str = Field(..., example="TX10298", description="ID unique de la transaction")
    user_id: str = Field(..., example="USR9018", description="ID unique de l'utilisateur")
    amount: float = Field(..., example=120.50, description="Montant de la transaction en EUR")
    merchant: str = Field(..., example="Amazon", description="Nom du marchand")
    device: str = Field(..., example="mobile", description="Canal de la transaction")
    timestamp: str = Field(..., example="2026-07-02T17:40:00Z", description="Horodatage ISO")

@app.get("/health")
def health():
    return {
        "status": "healthy",
        "kafka_connected": producer is not None
    }

@app.post("/ingest")
def ingest_event(payload: TransactionPayload):
    """
    Reçoit un événement et le pousse sur le topic Kafka 'transactions'
    """
    event = payload.dict()
    
    if producer is not None:
        try:
            # Envoi asynchrone à Kafka
            future = producer.send(KAFKA_TOPIC, value=event)
            # Attendre que le message soit écrit (optionnel pour haute performance, à désactiver en prod)
            future.get(timeout=2.0)
            return {"status": "success", "message": "Événement envoyé à Kafka", "transaction_id": payload.transaction_id}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Erreur d'écriture Kafka : {str(e)}")
    else:
        # Mode Mock si Kafka n'est pas lancé
        print(f"[MOCK INGESTION] : {event}")
        return {"status": "mock_success", "message": "Mode dégradé sans Kafka actif.", "data": event}
