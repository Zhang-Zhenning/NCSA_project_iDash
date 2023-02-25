set -e
set -x

# calling this script with two arguments will set the ports for the clickhouse server
# e.g. ./install_docker.sh 18123 19000

user_port1=18123
user_port2=19000

if [[ ${1+x} ]]; then
  user_port1="$1"
fi

if [[ ${2+x} ]]; then
  user_port2="$2"
fi


docker pull clickhouse/clickhouse-server
docker run -d -p ${user_port1}:8123 -p${user_port2}:9000 --name some-clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server

echo "ClickHouse server is running on port ${user_port1} and ${user_port2}"
echo "The docker container's id is: $(docker ps -q -f name=some-clickhouse-server)"

