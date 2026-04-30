import os
import subprocess
from pathlib import Path


def find_java_home():
    """Find the correct JAVA_HOME directory"""
    print("🔍 Finding Correct JAVA_HOME Path...")

    # Common Java installation paths
    possible_paths = [
        "C:\\Program Files\\Java",
        "C:\\Program Files\\Oracle\\Java",
        "C:\\Program Files (x86)\\Java",
        "C:\\Program Files (x86)\\Oracle\\Java"
    ]

    java_installations = []

    for base_path in possible_paths:
        if os.path.exists(base_path):
            print(f"📁 Checking: {base_path}")

            # Look for JDK directories
            try:
                for item in os.listdir(base_path):
                    item_path = os.path.join(base_path, item)
                    if os.path.isdir(item_path) and ('jdk' in item.lower() or 'java' in item.lower()):
                        # Check if it has bin/java.exe
                        java_exe = os.path.join(item_path, 'bin', 'java.exe')
                        if os.path.exists(java_exe):
                            java_installations.append(item_path)
                            print(f"   ✅ Found Java installation: {item_path}")

                            # Test the version
                            try:
                                result = subprocess.run([java_exe, '-version'],
                                                        capture_output=True, text=True)
                                if result.returncode == 0:
                                    version_info = result.stderr.split('\n')[0]
                                    print(f"      Version: {version_info}")
                            except:
                                pass

            except PermissionError:
                print(f"   ⚠️  Permission denied accessing {base_path}")
            except Exception as e:
                print(f"   ⚠️  Error accessing {base_path}: {e}")

    return java_installations


def get_java_from_registry():
    """Try to get Java path from Windows registry"""
    print("\n🔍 Checking Windows Registry for Java...")

    try:
        import winreg

        # Check registry for Java installation
        registry_paths = [
            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\JavaSoft\Java Development Kit"),
            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\JavaSoft\JDK"),
            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Oracle\Java Development Kit")
        ]

        for hkey, subkey in registry_paths:
            try:
                with winreg.OpenKey(hkey, subkey) as key:
                    # Get current version
                    current_version, _ = winreg.QueryValueEx(key, "CurrentVersion")
                    print(f"   Found Java version in registry: {current_version}")

                    # Get installation path for that version
                    version_key_path = f"{subkey}\\{current_version}"
                    with winreg.OpenKey(hkey, version_key_path) as version_key:
                        java_home, _ = winreg.QueryValueEx(version_key, "JavaHome")
                        print(f"   ✅ Registry JAVA_HOME: {java_home}")
                        return java_home

            except FileNotFoundError:
                continue
            except Exception as e:
                print(f"   ⚠️  Registry error: {e}")
                continue

    except ImportError:
        print("   ⚠️  winreg module not available")

    return None


def recommend_java_home():
    """Recommend the best JAVA_HOME path"""
    print("\n" + "=" * 60)
    print("📋 JAVA_HOME SETUP RECOMMENDATIONS")
    print("=" * 60)

    # Find installations
    installations = find_java_home()
    registry_path = get_java_from_registry()

    # Determine best path
    recommended_path = None

    if registry_path and os.path.exists(registry_path):
        recommended_path = registry_path
        print(f"\n✅ RECOMMENDED JAVA_HOME (from registry):")
        print(f"   {recommended_path}")
    elif installations:
        # Prefer the newest version (usually has highest path)
        recommended_path = max(installations)
        print(f"\n✅ RECOMMENDED JAVA_HOME (from file system):")
        print(f"   {recommended_path}")
    else:
        print(f"\n❌ No Java installations found automatically")

    if recommended_path:
        print(f"\n📝 SETUP INSTRUCTIONS:")
        print(f"1. Press Win + R, type 'sysdm.cpl', press Enter")
        print(f"2. Click 'Environment Variables' button")
        print(f"3. Under 'System Variables', click 'New'")
        print(f"4. Variable name: JAVA_HOME")
        print(f"5. Variable value: {recommended_path}")
        print(f"6. Click OK and restart your terminal/IDE")

        print(f"\n🧪 VERIFICATION COMMANDS:")
        print(f"   echo %JAVA_HOME%")
        print(f"   java -version")

        # Create a batch file for easy setup
        batch_content = f'''@echo off
echo Setting JAVA_HOME to: {recommended_path}
setx JAVA_HOME "{recommended_path}" /M
echo JAVA_HOME has been set. Please restart your terminal/IDE.
pause
'''

        try:
            with open('set_java_home.bat', 'w') as f:
                f.write(batch_content)
            print(f"\n💾 Created 'set_java_home.bat' for automated setup")
            print(f"   Run as Administrator to automatically set JAVA_HOME")
        except Exception as e:
            print(f"   ⚠️  Could not create batch file: {e}")

    else:
        print(f"\n💡 MANUAL SETUP NEEDED:")
        print(f"1. Look for Java installation in:")
        print(f"   - C:\\Program Files\\Java\\")
        print(f"   - C:\\Program Files\\Oracle\\")
        print(f"2. Find directory containing 'bin\\java.exe'")
        print(f"3. Use that directory as JAVA_HOME")


if __name__ == "__main__":
    recommend_java_home()