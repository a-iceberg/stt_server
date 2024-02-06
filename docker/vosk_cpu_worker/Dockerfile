# Use an official Python runtime as a parent image
FROM python:3.11

# Set the working directory in the container
WORKDIR /worker

# Install FFmpeg
RUN apt-get update && \
    apt-get install -y --no-install-recommends software-properties-common && \
    add-apt-repository ppa:mc3man/trusty-media -y && \
    apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    apt-get clean && \
    apt-get autoclean && \
    apt-get autoremove --yes && \
    rm -rf /var/cache/apt/archives/*.deb /var/cache/apt/archives/partial/*.deb /var/cache/apt/*.bin /var/lib/apt/lists/*

# Update pip and install Python dependencies
COPY requirements.txt /worker/
RUN pip3 install --upgrade pip && \
    pip3 install -r requirements.txt --no-cache-dir

# Create a directory for files
RUN mkdir /worker/files

# Copy the current directory contents into the container at /worker
COPY vosk_cpu_worker.py /worker/
COPY init_server.py /worker/

# Define the entrypoint and default command
ENTRYPOINT ["python3"]
CMD ["vosk_cpu_worker.py"]
