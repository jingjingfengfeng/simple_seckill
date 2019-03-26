#FROM seckill-server-tiny_grpctiny:latest

FROM zuisuiwoxin/seckill_env:v2

COPY src/seckill_server /home/server/
COPY src/start_server.sh /home/server/


CMD chmod +x /home/server/seckill_server 
CMD /home/server/start_server.sh
