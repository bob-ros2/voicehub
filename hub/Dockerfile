# ROS-based hub: the internal ROS-TTS mode (default) needs no edge container.
# For split-host operation (hub away from agent-q3tts): set VOICEHUB_ROS_TTS=0
# and start edge/ instead — the python:3.11-slim image then suffices.
ARG ROS_DISTRO=humble
FROM ros:${ROS_DISTRO}-ros-base

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    ROS_DISTRO=${ROS_DISTRO}

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-aiohttp \
    ros-${ROS_DISTRO}-rclpy \
    ros-${ROS_DISTRO}-std-msgs \
    && rm -rf /var/lib/apt/lists/*

# UID 1000 like agent-q3tts: FastRTPS shm discovery on the shared host.
RUN useradd -m -u 1000 -s /bin/bash ros 2>/dev/null || true

WORKDIR /app
COPY server.py /app/server.py
RUN chown -R ros:ros /app

USER ros
EXPOSE 8790
ENTRYPOINT ["/bin/bash", "-c", "source /opt/ros/${ROS_DISTRO}/setup.bash && exec python3 /app/server.py"]
