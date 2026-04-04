FROM rabbitmq:3.13-management

# Stream protocol (port 5552) + management UI for streams
RUN rabbitmq-plugins enable --offline rabbitmq_stream rabbitmq_stream_management

EXPOSE 4369 5672 5552 15672 25672
