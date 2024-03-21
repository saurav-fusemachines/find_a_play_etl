FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed dependencies specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the bash script into the container
COPY entrypoint.sh /app/run_scraping.sh

# Make the entrypoint script executable
RUN chmod +x /app/entrypoint.sh

# Run the bash script before executing the Python script
CMD ["/bin/bash", "./entrypoint.sh"]