self: super: rec {
  hadoop = super.hadoop_3_1;

  pageRankBench = super.callPackage ./bench/default.nix {
    jdk = super.jdk8;
  };

  spark3 = super.callPackage ./spark3/default.nix {
    inherit hadoop;
    jre = super.jre8;
  };
}
