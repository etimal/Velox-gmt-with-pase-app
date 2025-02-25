import io
import os
from datetime import datetime

import pandas as pd
import streamlit as st

from data_cleaning.gmt_viajes_salida import clean_gmt_data
from data_cleaning.pase import clean_pase_data
from gmt_pase_comparison import comparison

# Flag to control local execution mode
LOCAL_EXECUTION = False  # Set to False for production deployment


def load_gmt_file(file):
    """Load and validate GM Transport Excel file"""
    try:
        if LOCAL_EXECUTION and file is None:
            # Load from local test directory when in local mode
            local_path = os.path.join("test", "src", "gmt_transport.xlsx")
            df = pd.read_excel(local_path)
        else:
            df = pd.read_excel(file)
        return df, None
    except Exception as e:
        return None, f"Error loading GM Transport file: {str(e)}"


def load_pase_file(file):
    """Load and validate PASE CSV file"""
    try:
        if LOCAL_EXECUTION and file is None:
            # Load from local test directory when in local mode
            local_path = os.path.join("test", "src", "pase_data.csv")
            df = pd.read_csv(local_path, sep=",", encoding="utf-8")
        else:
            df = pd.read_csv(file, sep=",", encoding="utf-8")
        return df, None
    except Exception as e:
        return None, f"Error loading PASE file: {str(e)}"


def display_dataframe_info(df, title):
    """Display information about a dataframe"""
    st.write(f"🔹 {title} Info:")
    st.write(f"  • Number of rows: {len(df)}")
    st.write(f"  • Number of columns: {len(df.columns)}")
    st.write(f"  • Columns: {', '.join(df.columns)}")


def main():
    st.title("Data Comparison Tool")

    # Initialize session state
    if "gmt_transport_df" not in st.session_state:
        st.session_state.gmt_transport_df = None
    if "pase_df" not in st.session_state:
        st.session_state.pase_df = None
    if "cleaned_gmt_df" not in st.session_state:
        st.session_state.cleaned_gmt_df = None
    if "cleaned_pase_df" not in st.session_state:
        st.session_state.cleaned_pase_df = None

    # File uploaders in columns
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("GM Transport File (Excel)")
        if not LOCAL_EXECUTION:
            gmt_file = st.file_uploader(
                "Upload GM Transport file", type=["xlsx", "xls"], key="gm_upload"
            )
        else:
            st.info("Running in local mode - Using test/src/gmt_transport.xlsx")
            gmt_file = None

        df, error = load_gmt_file(gmt_file)

        if error:
            if gmt_file is None:
                st.warning("Please upload file")
            else:
                st.error(error)
        else:
            st.session_state.gmt_transport_df = df
            st.success("GM Transport file loaded successfully!")
            display_dataframe_info(df, "Original GM Transport Data")

            # Clean GMT data
            try:
                st.session_state.cleaned_gmt_df = clean_gmt_data(df)
                st.success("GM Transport data cleaned successfully!")
                display_dataframe_info(
                    st.session_state.cleaned_gmt_df, "Cleaned GM Transport Data"
                )
            except Exception as e:
                st.error(f"Error cleaning GM Transport data: {str(e)}")

    with col2:
        st.subheader("PASE File (CSV)")
        if not LOCAL_EXECUTION:
            pase_file = st.file_uploader(
                "Upload PASE file", type=["csv"], key="pase_upload"
            )
        else:
            st.info("Running in local mode - Using test/src/pase_data.csv")
            pase_file = None

        df, error = load_pase_file(pase_file)
        if error:
            if pase_file is None:
                st.warning("Please upload file")
            else:
                st.error(error)
        else:
            st.session_state.pase_df = df
            st.success("PASE file loaded successfully!")
            display_dataframe_info(df, "Original PASE Data")

            # Clean PASE data
            try:
                st.session_state.cleaned_pase_df = clean_pase_data(df)
                st.success("PASE data cleaned successfully!")
                display_dataframe_info(
                    st.session_state.cleaned_pase_df, "Cleaned PASE Data"
                )
            except Exception as e:
                st.error(f"Error cleaning PASE data: {str(e)}")

    # Process files if both are cleaned and ready
    if (
        st.session_state.cleaned_gmt_df is not None
        and st.session_state.cleaned_pase_df is not None
    ):
        st.subheader("Process Files")

        # Add export format selection
        export_format = st.radio("Select export format:", ("Excel", "CSV"))
        if LOCAL_EXECUTION:
            export_format = "CSV"

        download_status = st.button("Process and Download")
        if LOCAL_EXECUTION:
            download_status = True  # For testing purposes
        if download_status:
            try:
                # Run comparison on cleaned data
                result_df = comparison(
                    st.session_state.cleaned_gmt_df, st.session_state.cleaned_pase_df
                )

                # Generate timestamp for filename
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

                # Create the export file
                if export_format == "Excel":
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                        result_df.to_excel(writer, index=False)
                    output.seek(0)
                    file_extension = "xlsx"
                    mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                else:  # CSV
                    output = io.StringIO()
                    result_df.to_csv(output, index=False)
                    output = io.BytesIO(output.getvalue().encode())
                    file_extension = "csv"
                    mime_type = "text/csv"

                    if LOCAL_EXECUTION:
                        # Save to local test directory for testing
                        local_path = os.path.join("test", f"gmt_pase_{timestamp}.csv")
                        result_df.to_csv(
                            local_path, index=False, encoding="utf-8", sep=","
                        )
                        st.info(f"Saved output to {local_path}")

                # Create download button
                filename = f"gmt_pase_{timestamp}.{file_extension}"
                st.download_button(
                    label="Download Results",
                    data=output,
                    file_name=filename,
                    mime=mime_type,
                )

                st.success("Processing completed successfully!")

            except Exception as e:
                st.error(f"Error during processing: {str(e)}")

    # Clear data button
    if st.button("Clear All Data"):
        st.session_state.gmt_transport_df = None
        st.session_state.pase_df = None
        st.session_state.cleaned_gmt_df = None
        st.session_state.cleaned_pase_df = None


if __name__ == "__main__":
    main()
