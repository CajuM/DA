{ nrSparkWorkers, vcores, ram }:

let
  lib = import <nixpkgs/lib>;

  default = {
    deployment.targetEnv = "virtualbox";
    deployment.virtualbox.headless = true;
    deployment.virtualbox.memorySize = ram;
    deployment.virtualbox.vcpu = vcores;
  };

  makeSparkWorker = n: lib.attrsets.nameValuePair "spark-worker-${toString n}" default;

in

{
  spark-master = default;
} // builtins.listToAttrs (map makeSparkWorker (lib.lists.range 1 nrSparkWorkers))
