import os
import requests
import streamlit as st
from datetime import date

API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(page_title="Flight Delay Prediction", page_icon="✈️", layout="centered")
st.title("✈️ Prédiction de retard de vol")
st.markdown("Connectez-vous puis recherchez si votre vol sera en retard.")

if "token" not in st.session_state:
    st.session_state.token = None
if "airports" not in st.session_state:
    st.session_state.airports = {}

def login(username, password):
    try:
        r = requests.post(f"{API_URL}/auth/login", data={"username": username, "password": password})
        if r.status_code == 200:
            st.session_state.token = r.json()["access_token"]
            return True
        return False
    except:
        return False

def get_airports():
    try:
        r = requests.get(f"{API_URL}/airports", headers={"Authorization": f"Bearer {st.session_state.token}"})
        return {a["city"]: a["code"] for a in r.json()} if r.status_code == 200 else {}
    except:
        return {}

def get_destinations(origin_code):
    try:
        r = requests.get(f"{API_URL}/flights/destinations", headers={"Authorization": f"Bearer {st.session_state.token}"}, params={"origin_airport": origin_code})
        return {a["city"]: a["code"] for a in r.json()} if r.status_code == 200 else {}
    except:
        return {}

def get_hours(origin_code, destination_code, flight_date):
    try:
        r = requests.get(f"{API_URL}/flights/hours", headers={"Authorization": f"Bearer {st.session_state.token}"}, params={"origin_airport": origin_code, "destination_airport": destination_code, "flight_date": str(flight_date)})
        return r.json() if r.status_code == 200 else []
    except:
        return []

def get_prediction(flight_date, dep_hour, origin, destination):
    try:
        r = requests.get(f"{API_URL}/flights/prediction", headers={"Authorization": f"Bearer {st.session_state.token}"}, params={"flight_date": str(flight_date), "dep_hour": dep_hour, "origin_airport": origin, "destination_airport": destination})
        return r.json()
    except Exception as e:
        return {"error": str(e)}

if not st.session_state.token:
    st.subheader("🔐 Connexion")
    with st.form("login_form"):
        username = st.text_input("Nom d'utilisateur")
        password = st.text_input("Mot de passe", type="password")
        if st.form_submit_button("Se connecter"):
            if login(username, password):
                st.session_state.airports = get_airports()
                st.success("Connexion réussie !")
                st.rerun()
            else:
                st.error("Nom d'utilisateur ou mot de passe incorrect.")
else:
    st.subheader("🔍 Rechercher une prédiction")

    col1, col2 = st.columns(2)

    with col1:
        flight_date = st.date_input(
            "📅 Date du vol",
            value=date.today(),
            min_value=date.today(),
            max_value=date.today(),
        )

    with col2:
        origin_city = st.selectbox("🛫 Aéroport d'origine", options=list(st.session_state.airports.keys()))
        origin_code = st.session_state.airports.get(origin_city, "")
        destinations = get_destinations(origin_code) if origin_code else {}
        if destinations:
            destination_city = st.selectbox("🛬 Aéroport de destination", options=list(destinations.keys()))
            destination_code = destinations[destination_city]
        else:
            st.warning("Aucune destination disponible pour cet aéroport.")
            destination_city = None
            destination_code = None

    if destination_code:
        hours = get_hours(origin_code, destination_code, flight_date)
        if hours:
            dep_hour = st.selectbox("🕐 Heure de départ", options=hours, format_func=lambda x: f"{x:02d}h00")
        else:
            st.warning("Aucun vol disponible pour cette date et cette route.")
            dep_hour = None

    if st.button("Rechercher", type="primary") and destination_code and dep_hour is not None:
        with st.spinner("Recherche en cours..."):
            result = get_prediction(flight_date, dep_hour, origin_code, destination_code)
        if "error" in result:
            st.error(f"Erreur : {result['error']}")
        elif "message" in result:
            st.info(f"ℹ️ {result['message']}")
            st.write(f"**Date :** {result['flight_date']} | **Départ :** {origin_city} à {dep_hour:02d}h00 | **Arrivée :** {destination_city}")
        else:
            if result["is_delayed"]:
                st.error("🔴 Ce vol est prédit en **RETARD**")
            else:
                st.success("🟢 Ce vol est prédit **À L'HEURE**")
            st.divider()
            st.write(f"**Date :** {result['flight_date']} | **Départ :** {origin_city} à {dep_hour:02d}h00 | **Arrivée :** {destination_city}")

    st.divider()
    if st.button("Se déconnecter", type="secondary"):
        st.session_state.token = None
        st.session_state.airports = {}
        st.rerun()