from get_permission_data import get_permission_data
from analyze_permissions_data import analyze_permissions
from generate_pivot import generate_pivot


def main():

    # # Get all permissions
    # permissions_file = get_permission_data()
    permissions_file = None

    # Analyze permissions into matrix
    analyze_file = analyze_permissions(permissions_file)

    # Create Pivot output
    generate_pivot(analyze_file)


if __name__ == "__main__":
    main()
