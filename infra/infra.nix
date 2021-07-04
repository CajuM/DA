{ nrSparkWorkers, vcores, ram, ... }:

let
  lib = import <nixpkgs/lib>;
  overlay = import ./pkgs/overlay.nix;

  sparkMasterHostname = "spark-master";

  makeSparkNode =
    master: n:
      let
        hostname = if master then sparkMasterHostname else "spark-worker-${toString n}";

      in

      lib.attrsets.nameValuePair hostname (
      { config, pkgs, nodes, ... }:

      let
	hadoopConf = import <nixpkgs/nixos/modules/services/cluster/hadoop/conf.nix> {
	  inherit pkgs;
	  hadoop = config.services.hadoop;
	};

	sparkMasterIP = nodes."${sparkMasterHostname}".config.networking.privateIPv4;

      in

      {
        nixpkgs.overlays = [ overlay ];

	environment.variables = {
	  HADOOP_CONF_DIR = "${hadoopConf}";
	};

        environment.systemPackages = [ ] ++ (lib.optionals (master) [
	  pkgs.hadoop
	  pkgs.spark3
	  pkgs.piBench
	]);

        networking.firewall.enable = false;

	networking.hosts = lib.mkForce (
	  lib.lists.fold (e: a: a // e) { }
            (lib.attrsets.mapAttrsToList (n: v:
              { "${v.config.networking.privateIPv4}" = [ n ]; }
            ) nodes)
	);

	networking.nameservers = [ "${sparkMasterIP}" ];

	services = {
	  hadoop = {
	    hdfs.datanode.enabled = !master;
	    hdfs.namenode.enabled = master;

	    yarn.resourcemanager.enabled = master;
  	    yarn.nodemanager.enabled = !master;

	    coreSite = {
	      "fs.defaultFS" = "hdfs://${sparkMasterHostname}/";
	    };

	    hdfsSite = {
	      "dfs.permissions" = "false";
	    };

            yarnSite = {
	      "yarn.log-aggregation-enable" = "true";
              "yarn.resourcemanager.hostname" = sparkMasterHostname;
	      "yarn.resourcemanager.resource-tracker.nm.ip-hostname-check" = "false";
              "yarn.scheduler.capacity.root.queues" = "default";
              "yarn.scheduler.capacity.root.default.capacity" = 100;
	      "yarn.scheduler.maximum-allocation-mb" = 102400;
	      "yarn.scheduler.maximum-allocation-vcores" = 100;
            } // (lib.optionalAttrs (!master) {
  	      "yarn.nodemanager.log-dirs" = "/var/log/yarn/userlogs";
	      "yarn.nodemanager.resource.memory-mb" = ram;
	      "yarn.nodemanager.resource.cpu-vcores" = vcores;
	    });
	  };
	};

	systemd = {
	  tmpfiles.rules = [
	    "d /var/log/yarn 0770 root hadoop -"
      	  ] ++ (lib.optionals (master) [
	    "L+ /root/pi-bench.jar - - - - ${pkgs.piBench}/share/java/pi-bench.jar"
	  ]);
	} // (if master then {
	  services.hdfs-namenode = {
            after = [ "network.target" ];
            wants = [ "network.target" ];
	    
	    serviceConfig.Restart = "always";
	  };

	  services.yarn-resourcemanager = {
            after = [ "network.target" ];
            wants = [ "network.target" ];
	    
	    serviceConfig.Restart = "always";
	  };
	} else {
	  services.hdfs-datanode = {
            after = [ "network.target" ];
            wants = [ "network.target" ];
	    
	    serviceConfig.Restart = "always";
	  };

	  services.yarn-nodemanager = {
            after = [ "network.target" ];
            wants = [ "network.target" ];
	    
	    serviceConfig.Restart = "always";
	  };
        });
      }
    );

in

{  network = { }; } //
builtins.listToAttrs [ (makeSparkNode true 0) ] //
builtins.listToAttrs (map (makeSparkNode false) (lib.lists.range 1 nrSparkWorkers))
