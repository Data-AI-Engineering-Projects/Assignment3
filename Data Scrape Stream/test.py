data = [{"id": f"chunk-{i}", "values": embedding, "metadata": {"text": chunk}} for i, (chunk, embedding) in enumerate(zip(chunks, chunk_embeddings))]
pinecone_index.upsert(vectors=data)