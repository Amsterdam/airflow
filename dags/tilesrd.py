import airflow
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from airflow.kubernetes.secret import Secret

### Configmap Trex

configmap ="trex-env"
env_from = [k8s.V1EnvFromSource(
                config_map_ref=k8s.V1ConfigMapEnvSource(
                    name=configmap
                )
            )]

### Volume mounts

volume_mount_data_trex = k8s.V1VolumeMount(
    name='trex-volume-data', mount_path='/var/cache/mvtcache', sub_path=None, read_only=False)

volume_mount_data_mapproxy_rd = k8s.V1VolumeMount(
    name='mapproxy-volume-data-rd', mount_path='/mnt/tiles', sub_path=None, read_only=False)
volume_mount_data_mapproxy_rd_light = k8s.V1VolumeMount(
    name='mapproxy-volume-data-rd-light', mount_path='/mnt/tiles', sub_path=None, read_only=False)
volume_mount_data_mapproxy_rd_zw = k8s.V1VolumeMount(
    name='mapproxy-volume-data-rd-zw', mount_path='/mnt/tiles', sub_path=None, read_only=False)

volume_mount_config = k8s.V1VolumeMount(
    name='volume-cm', mount_path='/var/config', sub_path=None, read_only=True)

### Volume claims

volume_data_trex = k8s.V1Volume(
    name='trex-volume-data',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='trex-volume-data'),
)

volume_data_mapproxy_rd = k8s.V1Volume(
    name='mapproxy-volume-data-rd',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='mapproxy-volume-data-rd'),
)
volume_data_mapproxy_rd_zw = k8s.V1Volume(
    name='mapproxy-volume-data-rd-zw',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='mapproxy-volume-data-rd-zw'),
)
volume_data_mapproxy_rd_light = k8s.V1Volume(
    name='mapproxy-volume-data-rd-light',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='mapproxy-volume-data-rd-light'),
)

volume_config_trex = k8s.V1Volume(
    name='volume-cm',
    config_map=k8s.V1ConfigMapVolumeSource(name='trex-config'),
)

volume_config_mapproxy = k8s.V1Volume(
    name='volume-cm',
    config_map=k8s.V1ConfigMapVolumeSource(name='mapproxy-config'),
)

### Pods resource config

fullresources=k8s.V1ResourceRequirements(
    requests={
        'memory': '6Gi',
        'cpu': '2500m',
    },
    limits={
        'memory': '10Gi',
        'cpu': '3000m',
    }
)

### DAG Start
with DAG(
    dag_id='tilesrd',
    schedule_interval=None,
    start_date=days_ago(2),
) as dag:
    # trex_generate_vector_rd = KubernetesPodOperator(
    #     name="trex_generate_vector_rd",
    #     labels={"aadpodidbinding": "pio-tiles-id"},
    #     image="sourcepole/t-rex",
    #     namespace="airflow",
    #     arguments=["generate", "--progress", "false", "--overwrite", "true", "--minzoom", "5", "--maxzoom", "11", "--extent", "4.49712476945351,52.1630507756721,5.60867873764429,52.6147675426215", "--config", "/var/config/topo_rd.toml"],
    #     task_id="trex_generate_vector_rd",
    #     volumes=[volume_config_trex, volume_data_trex],
    #     volume_mounts=[volume_mount_config, volume_mount_data_trex],
    #     security_context=dict(fsGroup=33),
    #     env_from=env_from,
    #     node_selector={"nodetype": "tiles"},
    #     is_delete_operator_pod=False,
    #     startup_timeout_seconds=600,
    #     resources=fullresources,
    #     get_logs=True,
    #     do_xcom_push=False
    # )
    # upload_vector_rd = KubernetesPodOperator(
    #     name="upload_vector_rd",
    #     labels={"aadpodidbinding": "pio-tiles-id"},
    #     image="hawaku/azcopy",
    #     namespace="airflow",
    #     cmds=["/bin/bash"],
    #     arguments=["-c", "azcopy login --identity --identity-client-id 60efcd71-1ca4-4650-ba7b-66f04c720d75; sleep 2m; azcopy copy '/var/cache/mvtcache/rd/*' https://piosupportstor.blob.core.windows.net/tiles/rd/ --recursive --content-encoding gzip --content-type application/vnd.mapbox-vector-tile"],
    #     task_id="upload_vector_rd",
    #     volumes=[volume_data_trex],
    #     volume_mounts=[volume_mount_data_trex],
    #     security_context=dict(fsGroup=101),
    #     node_selector={"nodetype": "tiles"},
    #     is_delete_operator_pod=False,
    #     startup_timeout_seconds=600,
    #     get_logs=True,
    #     do_xcom_push=False
    # )    
    mapproxy_generate_tiles_rd = KubernetesPodOperator(
        name="mapproxy_generate_tiles_rd",
        image="dsoapi.azurecr.io/mapproxy",
        namespace="airflow",
        arguments=["mapproxy-seed", "-c", "16", "-s", "/var/config/seed.yaml", "-f", "/var/config/mapproxy.yaml", "--seed=rd_kbk,rd_bgt"],
        task_id="mapproxy_generate_tiles_rd",
        volumes=[volume_config_mapproxy, volume_data_mapproxy_rd],
        volume_mounts=[volume_mount_config, volume_mount_data_mapproxy_rd],
        security_context=dict(fsGroup=101),
        node_selector={"nodetype": "tiles"},
        is_delete_operator_pod=False,
        resources=fullresources,
        startup_timeout_seconds=600,
        get_logs=True,
        do_xcom_push=False
    )
    # mapproxy_generate_tiles_rd_zw = KubernetesPodOperator(
    #     name="mapproxy_generate_tiles_rd_zw",
    #     image="dsoapi.azurecr.io/mapproxy",
    #     namespace="airflow",
    #     arguments=["mapproxy-seed", "-c", "16", "-s", "/var/config/seed.yaml", "-f", "/var/config/mapproxy.yaml", "--seed=rd_kbk_zw,rd_bgt_zw"],
    #     task_id="mapproxy_generate_tiles_rd_zw",
    #     volumes=[volume_config_mapproxy, volume_data_mapproxy_rd_zw],
    #     volume_mounts=[volume_mount_config, volume_mount_data_mapproxy_rd_zw],
    #     security_context=dict(fsGroup=101),
    #     node_selector={"nodetype": "tiles"},
    #     is_delete_operator_pod=False,
    #     resources=fullresources,
    #     startup_timeout_seconds=600,
    #     get_logs=True,
    #     do_xcom_push=False
    # )
    # mapproxy_generate_tiles_rd_light = KubernetesPodOperator(
    #     name="mapproxy_generate_tiles_rd_light",
    #     image="dsoapi.azurecr.io/mapproxy",
    #     namespace="airflow",
    #     arguments=["mapproxy-seed", "-c", "16", "-s", "/var/config/seed.yaml", "-f", "/var/config/mapproxy.yaml", "--seed=rd_kbk_light,rd_bgt_light"],
    #     task_id="mapproxy_generate_tiles_rd_light",
    #     volumes=[volume_config_mapproxy, volume_data_mapproxy_rd_light],
    #     volume_mounts=[volume_mount_config, volume_mount_data_mapproxy_rd_light],
    #     security_context=dict(fsGroup=101),
    #     node_selector={"nodetype": "tiles"},
    #     is_delete_operator_pod=False,
    #     resources=fullresources,
    #     startup_timeout_seconds=600,
    #     get_logs=True,
    #     do_xcom_push=False
    # )
    upload_tiles_rd = KubernetesPodOperator(
        name="upload_tiles_rd",
        labels={"aadpodidbinding": "pio-tiles-id"},
        image="hawaku/azcopy",
        namespace="airflow",
        cmds=["/bin/bash"],
        arguments=["-c", "azcopy login --identity --identity-client-id 60efcd71-1ca4-4650-ba7b-66f04c720d75; azcopy copy '/mnt/tiles/cache_rd_seed_EPSG28992/*' https://piosupportstor.blob.core.windows.net/tiles/rd/default/ --recursive"],
        task_id="upload_tiles_rd",
        volumes=[volume_data_mapproxy_rd],
        volume_mounts=[volume_mount_data_mapproxy_rd],
        security_context=dict(fsGroup=101),
        node_selector={"nodetype": "tiles"},
        is_delete_operator_pod=False,
        startup_timeout_seconds=600,
        get_logs=True,
        do_xcom_push=False
    )
    # upload_tiles_rd_zw = KubernetesPodOperator(
    #     name="upload_tiles_rd_zw",
    #     labels={"aadpodidbinding": "pio-tiles-id"},
    #     image="hawaku/azcopy",
    #     namespace="airflow",
    #     cmds=["/bin/bash"],
    #     arguments=["-c", "azcopy login --identity --identity-client-id 60efcd71-1ca4-4650-ba7b-66f04c720d75; azcopy copy '/mnt/tiles/cache_rd_seed_zw_EPSG28992/*' https://piosupportstor.blob.core.windows.net/tiles/rd/zw/ --recursive"],
    #     task_id="upload_tiles_rd_zw",
    #     volumes=[volume_data_mapproxy_rd_zw],
    #     volume_mounts=[volume_mount_data_mapproxy_rd_zw],
    #     security_context=dict(fsGroup=101),
    #     node_selector={"nodetype": "tiles"},
    #     is_delete_operator_pod=False,
    #     startup_timeout_seconds=600,
    #     get_logs=True,
    #     do_xcom_push=False
    # )
    # upload_tiles_rd_light = KubernetesPodOperator(
    #     name="upload_tiles_rd_light",
    #     labels={"aadpodidbinding": "pio-tiles-id"},
    #     image="hawaku/azcopy",
    #     namespace="airflow",
    #     cmds=["/bin/bash"],
    #     arguments=["-c", "azcopy login --identity --identity-client-id 60efcd71-1ca4-4650-ba7b-66f04c720d75; azcopy copy '/mnt/tiles/cache_rd_seed_light_EPSG28992/*' https://piosupportstor.blob.core.windows.net/tiles/rd/light/ --recursive"],
    #     task_id="upload_tiles_rd_light",
    #     volumes=[volume_data_mapproxy_rd_light],
    #     volume_mounts=[volume_mount_data_mapproxy_rd_light],
    #     security_context=dict(fsGroup=101),
    #     node_selector={"nodetype": "tiles"},
    #     is_delete_operator_pod=False,
    #     startup_timeout_seconds=600,
    #     get_logs=True,
    #     do_xcom_push=False
    # )

# trex_generate_vector_rd >> upload_vector_rd >> mapproxy_generate_tiles_rd >> mapproxy_generate_tiles_rd_zw >> mapproxy_generate_tiles_rd_light 
mapproxy_generate_tiles_rd >> upload_tiles_rd
# mapproxy_generate_tiles_rd_zw >> upload_tiles_rd_zw
# mapproxy_generate_tiles_rd_light >> upload_tiles_rd_light
