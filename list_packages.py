import pkg_resources

def list_installed_packages():
    installed_packages = sorted(
        [(d.project_name, d.version) for d in pkg_resources.working_set]
    )
    for name, version in installed_packages:
        print(f"{name}=={version}")

if __name__ == "__main__":
    list_installed_packages()
