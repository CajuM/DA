self: super: rec {
  hadoop = super.hadoop_3_1;

  spark3 = super.callPackage ./spark3/default.nix {
    hadoop = hadoop;
    jre = super.jre8;
  };

  pageRankBench = super.callPackage ../../default.nix {
    jdk = super.jdk8;
  };
}
