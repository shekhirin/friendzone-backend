export WORKSPACE = $(pwd)
export GOBIN=$WORKSPACE/bin

dep ensure

sudo cp parser/supervisor.conf /etc/supervisor/conf.d/parser.conf
supervisorctl reload