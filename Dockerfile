FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir pip --upgrade

COPY pyproject.toml .
COPY deriv_mcp/ deriv_mcp/

RUN pip install --no-cache-dir .

COPY .env* ./

CMD ["python", "-m", "deriv_mcp.server"]
