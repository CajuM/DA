{ nrSparkWorkers ? 0
, vcores
, ram
, rootDiskSize
, instanceType
, accessKeyId
, region }:

let
  lib = import <nixpkgs/lib>;

  default = { resources, ... }: {
    deployment.targetEnv = "ec2";
    deployment.ec2.accessKeyId = accessKeyId;
    deployment.ec2.ebsBoot = true;
    deployment.ec2.ebsInitialRootDiskSize = rootDiskSize;
    deployment.ec2.instanceType = instanceType;
    deployment.ec2.keyPair = resources.ec2KeyPairs.keypair;
    deployment.ec2.region = region;
  };

  makeSparkWorker = n: lib.attrsets.nameValuePair "spark-worker-${toString n}" default;

  spark-master = {
    spark-master = default;
  };

  spark-workers = builtins.listToAttrs (map makeSparkWorker (lib.lists.range 1 nrSparkWorkers));

in

{
  resources.ec2KeyPairs.keypair = { inherit region accessKeyId; };
} // spark-master // spark-workers
