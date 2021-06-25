{ lib
, stdenv
, gradleGen
, jdk
, perl
}:

let
  pname = "salsa-bench";
  version = "1";

  src = builtins.filterSource (path: type: !(
    (baseNameOf path == "infra") || (baseNameOf path == "dataset") || (baseNameOf path == "build")
  )) ./.;

  gradle = (gradleGen.override (old: { java = jdk; })).gradle_7;

  # fake build to pre-download deps into fixed-output derivation
  deps = stdenv.mkDerivation {
    pname = "${pname}-deps";
    inherit src version;

    nativeBuildInputs = [ gradle perl ];
    buildPhase = ''
      export GRADLE_USER_HOME=$(mktemp -d)
      gradle --no-daemon --no-watch-fs shadowJar
    '';
    # perl code mavenizes paths (com.squareup.okio/okio/1.13.0/a9283170b7305c8d92d25aff02a6ab7e45d06cbe/okio-1.13.0.jar -> com/squareup/okio/okio/1.13.0/okio-1.13.0.jar)
    installPhase = ''
      find $GRADLE_USER_HOME/caches/modules-2 -type f -regex '.*\.\(jar\|pom\)' \
        | perl -pe 's#(.*/([^/]+)/([^/]+)/([^/]+)/[0-9a-f]{30,40}/([^/\s]+))$# ($x = $2) =~ tr|\.|/|; "install -Dm444 $1 \$out/$x/$3/$4/$5" #e' \
        | sh
    '';
    outputHashAlgo = "sha256";
    outputHashMode = "recursive";
    outputHash = "068cay20hlmnylwxrj1h3inxc4b8qi2b95510a50mz2plb6fkr93";
  };

in

stdenv.mkDerivation rec {
  inherit pname src version;

  buildInputs = [ ];
  nativeBuildInputs = [
    gradle
    jdk
  ];

  buildPhase = with lib; ''
    export GRADLE_USER_HOME=$(mktemp -d)

    # point to offline repo
    sed -ie "s#mavenCentral()#maven { url '${deps}' }#g" build.gradle
    sed -ie "s#gradlePluginPortal()#maven { url '${deps}' }#g" settings.gradle

    gradle --no-watch-fs --offline --no-daemon shadowJar -Pbuildversion=${version}
  '';

  installPhase = with lib; ''
    mkdir -p $out/share/java
    install -Dm644 build/libs/salsa-bench-all.jar $out/share/java/salsa-bench.jar
  '';
}
