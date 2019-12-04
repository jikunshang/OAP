#!/bin/bash

SPARK_UPSTREAM_URL="https://github.com/apache/spark/archive/"
SPARK_VERSION_SUPPORTED=(
  "v2.3.2"
)

OAP_HOME=$(dirname `pwd`)
DOWNLOADED_SPARK_DIR=$(dirname $OAP_HOME)
echo $DOWNLOADED_SPARK_DIR
PATCH_DIR=$OAP_HOME/patches

function echo_usage() {
  cat << EOF
  Usage:
    ${0} -c <path>
    ${0} -v <spark_version>
    -c | --custom-spark   - OPTIONAL. Patch to the customed spark. It you don't set, we will download from upstream
    -v | --version        - REQUIRED. Patch to the upstream spark, current we support ${SPARK_VERSION_SUPPORTED}
                            Also you can select one of version and try to apply to your spark

    For example:
      apply_patch.sh -v v2.3.2
      apply_patch.sh -v v2.3.2 -c YOUR_SPARK_DIR
EOF
exit 1
}

CURL=$(command -v curl)
check_curl_exist() {
  if ! [ -x "$CURL" ]; then
    echo "ERROR: curl is not found!"
    exit 1
  fi
}

GIT=$(command -v git)
check_git_exist() {
  if ! [ -x "$GIT" ]; then
    echo "ERROR: git is not found!"
    exit 1
  fi
}

MVN=$(command -v mvn)
check_mvn_exist() {
  if ! [ -x "$MVN" ]; then
    echo "ERROR: mvn is not found!"
    exit 1
  fi
}

TARBALL_SUFFIX=".tar.gz"
fetch_from_upstream() {
  check_curl_exist

  TARBALL_NAME=$UPSTREAM_SPARK_VERSION$TARBALL_SUFFIX
  mkdir -p $DOWNLOADED_SPARK_DIR/spark_source
  $CURL --fail -L $SPARK_UPSTREAM_URL$TARBALL_NAME \
          -o $TARBALL_NAME
  if [ $? != 0 ]; then
    echo "Please check whether the spark version is in support list ($SPARK_VERSION_SUPPORTED) and ensure the network available"
    exit 1
  fi

  SPARK_SOURCE_NAME=`tar -tf $TARBALL_NAME | head -1 | cut -f1 -d"/"`
  if [ $? != 0 ]; then
    exit 1
  fi
  SPARK_SOURCE_DIR=$DOWNLOADED_SPARK_DIR/spark_source/$SPARK_SOURCE_NAME
  if [ -d $SPARK_SOURCE_DIR ]; then
    rm -rf $SPARK_SOURCE_DIR
  fi
  tar -zxf $TARBALL_NAME -C $DOWNLOADED_SPARK_DIR/spark_source
  rm $TARBALL_NAME
}

patch() {
  # TODO: we'd better use /usr/bin/patch to apply patch. Here seems the current patch file is not applicable
  check_git_exist
  cd $SPARK_SOURCE_DIR

  if ! [ -d .git ]; then
    $GIT init
  fi

  $GIT apply $PATCH_DIR/$UPSTREAM_SPARK_VERSION/*.patch
  if [ $? != 0 ]; then
    echo "Fail to apply the patch. Please try to solve conflicts if you are using custom spark"
    exit 1
  fi
  echo "Apply patches successfully"
}

build() {
  check_mvn_exist
  cd $SPARK_SOURCE_DIR
  ./dev/make-distribution.sh --name spark --tgz -Phadoop-2.7 -Phive -Phive-thriftserver -Pyarn
  if [ $? != 0 ]; then
    echo "The spark tarball(.tgz) generated under "$SPARK_SOURCE_DIR
    echo "you can use the built spark here or replace the jar file under your own spark"
    exit 1
  fi
}

###################################################################################

OPTS=`getopt --options c:v: --long "custom-spark:,version:" -n "$0" -- "$@"`
if [ $? != 0 ]; then
  echo "Failed parsing options."
  echo_usage
  exit 1
fi

while true; do
  case "$1" in
    -c | --custom-spark)
      SPARK_SOURCE_DIR=$2; shift 2;;
    -v | --version)
      UPSTREAM_SPARK_VERSION=$2; shift 2;;
    --)
      shift; break;;
    *)
      break ;;
  esac
done

if [ -z "$UPSTREAM_SPARK_VERSION" ]; then
  echo_usage
fi

if [[ "${SPARK_VERSION_SUPPORTED[*]}" =~ $UPSTREAM_SPARK_VERSION ]]; then
  #echo "fetch_from_upstream"
  fetch_from_upstream
fi

patch
build
