(function() {
    let accessToken = null;
    const titleStyle = "font-weight: bold; font-size: 16px; color: #198754;"; // Verde
    const valueStyle = "font-family: monospace; color: #d63384; font-size: 12px;";

    // Itera sobre el localStorage para encontrar el access_token
    for (let i = 0; i < localStorage.length; i++) {
        const key = localStorage.key(i);
        if (key.includes('offline_access')) { // La clave que contiene el access_token
            try {
                const data = JSON.parse(localStorage.getItem(key));
                if (data && data.body && data.body.access_token) {
                    accessToken = data.body.access_token;
                    break;
                }
            } catch (e) {}
        }
    }

    console.clear();

    if (accessToken) {
        console.log("%c🔑 Access Token para la API (el que debes usar):", titleStyle);
        console.log("%c" + accessToken, valueStyle);
    } else {
        console.error("❌ No se encontró el access_token. Asegúrate de estar en la página correcta de IEB después de iniciar sesión.");
    }
})();
