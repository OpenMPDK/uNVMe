chmod 777 *

echo "nvme ssd fw download"
DEVICE=0000:02:00.0
FW=./fw.bin
NVME_CLI=./kv_nvme

${NVME_CLI} fw-download ${DEVICE} --fw=${FW}

sleep 2

echo NVMe Fw Activate

# Rom BOOT(Rom to Main)
# main-to-main : slot 2
# rom : slot 1
if [[ "$NVME_CLI" != *"kv_nvme" ]]; then 
    # this is only needed when users use kernel nvme-cli
    ${NVME_CLI} fw-activate ${DEVICE} --slot=2 --action=1
    #kv_nvme does fw-activate automatically after fw-download
fi;

sleep 2

echo Please Do Reboot Manually
# reboot
