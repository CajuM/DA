{ myPkgs ? import <nixpkgs> {
  config.packageOverrides = (pkgs: {
    nixopsUnstable = pkgs.nixopsUnstable.override {
      overrides = (self: super: {
        nixops-aws = super.nixops-aws.overrideAttrs (_: {
	  patches = [ (pkgs.writeText "mkFixStrictness.patch" ''
            diff --color -Nur nixops-aws-b/nixops_aws/nix/ec2.nix nixops-aws/nixops_aws/nix/ec2.nix
            --- nixops-aws-b/nixops_aws/nix/ec2.nix 2021-07-02 15:42:16.606542857 +0300
            +++ nixops-aws/nixops_aws/nix/ec2.nix   2021-07-02 15:40:57.437690552 +0300
            @@ -508,7 +508,7 @@
                 # Workaround: the evaluation of blockDeviceMapping requires fileSystems to be defined.
                 fileSystems = {};

            -    deployment.ec2.blockDeviceMapping = mkFixStrictness (listToAttrs
            +    deployment.ec2.blockDeviceMapping = (listToAttrs
                   (map (fs: nameValuePair (dmToDevice fs.device)
                     { inherit (fs.ec2) disk size deleteOnTermination encrypt cipher keySize passphrase iops volumeType encryptionType;
                       fsType = if fs.fsType != "auto" then fs.fsType else fs.ec2.fsType;
	    '')
	  ];
	});
      });
    };
  });
} }:

let
  myGradle = (myPkgs.gradleGen.override (old: { java = myPkgs.jdk8; })).gradle_7;

  myPython = myPkgs.python38;

  myPythonEnv = myPython.withPackages (ps: with ps; [
    boto3
    botocore
  ]);

  pageRankBenchEnv = with myPkgs; stdenv.mkDerivation {
    name = "pagerank-bench-env";
    buildInputs = [
      jdk8
      myGradle
      myPythonEnv
      nixopsUnstable
    ];
  };

in pageRankBenchEnv
