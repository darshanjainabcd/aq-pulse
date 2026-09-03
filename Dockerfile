FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD [&quot;python&quot;, &quot;-m&quot;, &quot;unittest&quot;, &quot;discover&quot;, &quot;-s&quot;, &quot;tests&quot;, &quot;-v&quot;]
