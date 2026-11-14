# Homebrew formula for in-flight, published to a tap by the release workflow (.github/workflows/release.yml),
# which fills in the version, the URLs and their checksums. Install:
#   brew install <owner>/tap/in-flight
# then:
#   in-flight                                   # the app on http://localhost:8088
#   claude mcp add in-flight -- in-flight mcp
class InFlight < Formula
  desc "Console and MCP server for the money moving through your Kafka"
  homepage "https://github.com/@REPOSITORY@"
  version "@VERSION@"
  license "Apache-2.0"

  on_macos do
    on_arm do
      url "https://github.com/@REPOSITORY@/releases/download/v@VERSION@/in-flight-@VERSION@-macos-arm64.tar.gz"
      sha256 "@SHA_MACOS_ARM64@"
    end
    on_intel do
      url "https://github.com/@REPOSITORY@/releases/download/v@VERSION@/in-flight-@VERSION@-macos-x64.tar.gz"
      sha256 "@SHA_MACOS_X64@"
    end
  end
  on_linux do
    url "https://github.com/@REPOSITORY@/releases/download/v@VERSION@/in-flight-@VERSION@-linux-x64.tar.gz"
    sha256 "@SHA_LINUX_X64@"
  end

  def install
    # The app image carries its own Java runtime: nothing else to install.
    libexec.install Dir["*"]
    launcher = OS.mac? ? libexec/"in-flight.app/Contents/MacOS/in-flight" : libexec/"in-flight/bin/in-flight"
    bin.install_symlink launcher => "in-flight"
  end

  test do
    assert_match "in-flight", shell_output("#{bin}/in-flight version")
  end
end
