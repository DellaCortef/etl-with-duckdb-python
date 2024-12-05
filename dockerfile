# Specify the base image
FROM python:3.12

# Set environment variables to prevent Python from buffering
ENV PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create and set the working directory
WORKDIR /src

# Copy the application code into the container
COPY . /src

# Install Python dependencies
RUN pip install --no-cache-dir poetry
RUN poetry config virtualenvs.create false && poetry install --no-dev

# Expose the port Streamlit runs on
EXPOSE 8501

# Define the command to run the application
ENTRYPOINT ["poetry", "run", "streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
