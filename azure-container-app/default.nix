# For users who don't use flakes, provide a default.nix
{ pkgs ? import <nixpkgs> { } }:

let
  # Parse Cargo.toml to get package info
  cargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);
  pname = cargoToml.package.name;
  version = cargoToml.package.version;

  # Build dependencies
  buildInputs = with pkgs; [
    openssl
    pkg-config
  ] ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
    pkgs.darwin.apple_sdk.frameworks.Security
    pkgs.darwin.apple_sdk.frameworks.SystemConfiguration
  ];

  nativeBuildInputs = with pkgs; [
    rustc
    cargo
    pkg-config
  ];

in
pkgs.rustPlatform.buildRustPackage {
  inherit pname version;

  src = ./.;
  
  cargoLock = {
    lockFile = ./Cargo.lock;
  };

  inherit buildInputs nativeBuildInputs;

  # Set environment variables for OpenSSL
  OPENSSL_NO_VENDOR = 1;
  OPENSSL_LIB_DIR = "${pkgs.openssl.out}/lib";
  OPENSSL_INCLUDE_DIR = "${pkgs.openssl.dev}/include";

  meta = with pkgs.lib; {
    description = "Azure Container Apps health checker and revision manager";
    homepage = "https://github.com/sjeandeaux/azure-container-app";
    license = licenses.mit;
    maintainers = [ ];
    platforms = platforms.all;
  };
}