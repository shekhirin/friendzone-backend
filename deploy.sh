dep ensure
go install friendzone-backend/parser/

set -a
. ./.env
set +a

sudo cp parser/supervisor.conf /etc/supervisor/conf.d/parser.conf
supervisorctl reload