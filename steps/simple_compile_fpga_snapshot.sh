#! /bin/sh
#
# compile.sh

WORK_DIR=`pwd`
curl ftp://ftp.4pd.io/pub/team_hpc/titanse/pz-titanse-v0.1.0-release.tar.gz -o pz.tar.gz
tar zxf pz.tar.gz -C ./thirdparty
curl "https://nexus.4pd.io/repository/raw-hosted/fpga-rte/release/v0.2.1/aclrte-linux64-aclrte-19.2.0.57-v0.2.1-release.tar.gz" -O
tar zxf aclrte-linux64-aclrte-19.2.0.57-v0.2.1-release.tar.gz -C ./thirdparty
sed -i 's/\"Enable pz compression snapshot\"\ OFF/\"Enable pz compression snapshot\"\ ON/g' CMakeLists.txt
sh steps/gen_code.sh
mkdir -p $WORK_DIR/build
cd $WORK_DIR/build && cmake .. && make -j36 rtidb && mv bin/rtidb bin/rtidb_fpga
code=$?
cd $WORK_DIR
exit $code
