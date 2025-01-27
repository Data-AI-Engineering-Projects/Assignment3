import streamlit as st

# Check if a PDF is selected
if 'selected_pdf' in st.session_state and st.session_state['selected_pdf']:
    pdf_name, brief_summary, image_link, pdf_link = st.session_state['selected_pdf']

    # Show PDF details
    st.title(f"Details of {pdf_name}")
    st.image(image_link, width=400)  # Show the image
    st.write(f"**Summary**: {brief_summary}")
    st.markdown(f"[Open PDF]({pdf_link})", unsafe_allow_html=True)  # Link to open PDF

    # Back button to return to main page
    if st.button("Back to Main"):
        st.session_state['selected_pdf'] = None
        st.rerun()

else:
    st.write("No PDF selected.")