FROM public.ecr.aws/lambda/python:3.9

# Copy function code
COPY app.py ${LAMBDA_TASK_ROOT}
COPY cognito_preprocessing.py ${LAMBDA_TASK_ROOT}
COPY function_cognito_match.py ${LAMBDA_TASK_ROOT}
COPY us_zip_data.feather ${LAMBDA_TASK_ROOT}
COPY global-bundle.pem ${LAMBDA_TASK_ROOT}
COPY .env ${LAMBDA_TASK_ROOT}

# Install the function's dependencies using file requirements.txt
# from your project
RUN yum update -y
RUN yum -y install curl gcc
RUN curl -LsS -O https://downloads.mariadb.com/MariaDB/mariadb_repo_setup
RUN /bin/bash mariadb_repo_setup --os-type=rhel --os-version=7 --mariadb-server-version=10.6.9
RUN yum -y install MariaDB-devel
COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "app.handler" ]