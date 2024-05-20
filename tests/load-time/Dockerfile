#FROM qdrant/qdrant:latest
#use image with version of qdrant that has additional logging
FROM ghcr.io/qdrant/qdrant:test-load-time-tests

RUN apt-get update && apt-get install -y python3.11
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1

WORKDIR /
# mount load_time_calculation.py file, mri_output folder, snapshots folder and folder with mri binary into the container
CMD ["python3", "load_time_calculation.py", "--mri" , "/target/debug/mri", "--mri-output", "/mri_output", "--qdrant", "./qdrant/qdrant", "--snapshots", "/snapshots"]
