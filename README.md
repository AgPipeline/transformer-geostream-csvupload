# Transformer: BETYdb CSV file upload

Loads Trait data in CSV files to BETYdb.

### Sample Docker Command Line

Below is a sample command line that shows how the field mosaic Docker image could be run.
An explanation of the command line options used follows.
Be sure to read up on the [docker run](https://docs.docker.com/engine/reference/run/) command line for more information.

The files used in this sample command line can be found on [Google Drive](https://drive.google.com/file/d/1PSYmNnCjC3nxQp5os_95uhWx7MMMViTb/view?usp=sharing).

```sh
docker run --rm --mount "src=/home/test,target=/mnt,type=bind" -e "BETYDB_URL=<BETYdb URL>" -e "BETYDB_KEY=<BETYdb Key>" agpipeline/betydb-csvupload:2.0 --working_space "/mnt" "/mnt/ir_fullfield_L2_ua-mac_2018-06-03_stereovis_ir_sensors_partialplots_sorghum6_sun_flir_eastedge_mn_meantemp_bety.csv"
```

This example command line assumes the source files are located in the `/home/test` folder of the local machine.
The name of the image to run is `agpipeline/betydb-csvupload:2.0`.

We are using the same folder for the source files and the output files.
By using multiple `--mount` options, the source and output files can be separated.

**Docker commands** \
Everything between 'docker' and the name of the image are docker commands.

- `run` indicates we want to run an image
- `--rm` automatically delete the image instance after it's run
- `--mount "src=/home/test,target=/mnt,type=bind"` mounts the `/home/test` folder to the `/mnt` folder of the running image
- `-e "BETYDB_URL=<BETYdb URL>"` specifies the URL of the BETYdb instance to load data to
- `-e "BETYDB_KEY=<BETYdb Key>"` specifies the permission key used to access the BETYdb instance

We mount the `/home/test` folder to the running image to make files available to the software in the image.

**Image's commands** \
The command line parameters after the image name are passed to the software inside the image.
Note that the paths provided are relative to the running image (see the --mount option specified above).

- `--working_space "/mnt"` specifies the folder to use as a workspace
- `/mnt/ir_fullfield_L2_ua-mac_2018-06-03_stereovis_ir_sensors_partialplots_sorghum6_sun_flir_eastedge_mn_meantemp_bety.csv` is the name CSV file to upload
