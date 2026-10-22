ARG ROS_DISTRO=humble
FROM ros:${ROS_DISTRO}-ros-base

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    ROS_DISTRO=${ROS_DISTRO}

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip \
    ros-${ROS_DISTRO}-rclpy \
    ros-${ROS_DISTRO}-std-msgs \
    && rm -rf /var/lib/apt/lists/*

# Must match agent-q3tts (UID 1000) so FastRTPS shared memory discovery works
# correctly on the shared host.
RUN useradd -m -u 1000 -s /bin/bash ros 2>/dev/null || true

WORKDIR /app
COPY edge.py /app/edge.py
RUN chown -R ros:ros /app

USER ros
ENTRYPOINT ["/bin/bash", "-c", "source /opt/ros/${ROS_DISTRO}/setup.bash && exec python3 /app/edge.py"]
