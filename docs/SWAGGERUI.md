To set-up a swagger UI as an interface to your QDrant vector DB. Example dev docker-compose looks like: 

```
version: "3.3"
services:

  swagger-ui:
    image: swaggerapi/swagger-ui
    container_name: swagger_ui_container
    ports:
        - "8888:8080"
    volumes:
        - ./doc:/usr/share/nginx/html/doc
    environment:
        API_URL: doc/openapi.yaml

  qdrant-db:
    image: qdrant/qdrant:master
    ports:
      - "6333:6333"
      - "8502:6334"
    volumes:
      - ./path/to/data:/qdrant/storage
      - ./development.yaml:/qdrant/config/development.yaml
```
Where ... has been loaded to doc/openapi.yaml
