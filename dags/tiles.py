import airflow
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount
from airflow.kubernetes.secret import Secret

trex_configmaps = ['trex-env']

volume_mount_data_trex = VolumeMount(name='volume-data',
                            mount_path='/var/cache/mvtcache',
                            sub_path=None,
                            read_only=False)

volume_mount_data_mapproxy = VolumeMount(name='volume-data',
                            mount_path='/mnt/tiles',
                            sub_path=None,
                            read_only=False)

volume_mount_config = VolumeMount(name='volume-cm',
                            mount_path='/var/config',
                            sub_path=None,
                            read_only=True)

volume_data= {
    'persistentVolumeClaim':
      {
        'claimName': 'tiles-volume-data'
      }
    }

volume_cm_trex= {
    'configMap':
      {
        'name': 'trex-config'
      }
    }

volume_cm_mapproxy= {
    'configMap':
      {
        'name': 'mapproxy-config'
      }
    }

volume_config_trex = Volume(name='volume-cm', configs=volume_cm_trex)
volume_config_mapproxy = Volume(name='volume-cm', configs=volume_cm_mapproxy)
volume_data = Volume(name='volume-data', configs=volume_data)

with DAG(
    dag_id='tiles_wm',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['tiles'],
) as dag:
    trex_generate_pbf = KubernetesPodOperator(
        name="trex_generate_pbf",
        labels={"aadpodidbinding": "pio-tiles-id"},
        image="sourcepole/t-rex",
        namespace="tiles",
        arguments=["generate", "--maxzoom", "16", "--config", "/var/config/topo_wm.toml"],
        task_id="trex_generate_pbf",
        volumes=[volume_config_trex, volume_data],
        volume_mounts=[volume_mount_config, volume_mount_data_trex],
        security_context=dict(fsGroup=33),
        configmaps=trex_configmaps,
        get_logs=True,
        do_xcom_push=False
    )
    upload_pbf = KubernetesPodOperator(
        name="upload_pbf",
        labels={"aadpodidbinding": "pio-tiles-id"},
        image="hawaku/azcopy",
        namespace="tiles",
        cmds=["/bin/bash"],
        arguments=["-c", "azcopy login --identity --identity-client-id 60efcd71-1ca4-4650-ba7b-66f04c720d75; azcopy copy '/var/cache/mvtcache/*' https://piosupportstor.blob.core.windows.net/tiles/pbf/ --recursive --content-encoding gzip"],
        task_id="upload_pbf",
        volumes=[volume_data],
        volume_mounts=[volume_mount_data_trex],
        security_context=dict(fsGroup=101),
        get_logs=True,
        do_xcom_push=False
    )
    mapproxy_generate_tiles = KubernetesPodOperator(
        name="mapproxy_generate_tiles",
        image="dsoapi.azurecr.io/mapproxy",
        namespace="tiles",
        arguments=["mapproxy-seed", "-c", "2", "-s", "/var/config/seed.yaml", "-f", "/var/config/mapproxy.yaml", "--seed=wm_kbk"],
        task_id="mapproxy_generate_tiles",
        volumes=[volume_config_mapproxy, volume_data],
        volume_mounts=[volume_mount_config, volume_mount_data_mapproxy],
        security_context=dict(fsGroup=101),
        get_logs=True,
        do_xcom_push=False
    )
    upload_tiles = KubernetesPodOperator(
        name="upload_tiles",
        labels={"aadpodidbinding": "pio-tiles-id"},
        image="hawaku/azcopy",
        namespace="tiles",
        cmds=["/bin/bash"],
        arguments=["-c", "azcopy login --identity --identity-client-id 60efcd71-1ca4-4650-ba7b-66f04c720d75; azcopy copy '/mnt/tiles/cache_wm_seed_EPSG3857/*' https://piosupportstor.blob.core.windows.net/tiles/wm/ --recursive"],
        task_id="upload_tiles",
        volumes=[volume_data],
        volume_mounts=[volume_mount_data_mapproxy],
        security_context=dict(fsGroup=101),
        get_logs=True,
        do_xcom_push=False
    )

(
    trex_generate_pbf
    >> upload_pbf
    >> mapproxy_generate_tiles
    >> upload_tiles
)
