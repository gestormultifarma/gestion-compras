# 🧾 Sistema de Gestión de Compras - Multifarma

**Proyecto en desarrollo** para la automatización del proceso de análisis y gestión de compras en la cadena de droguerías **Multifarma**.

---

## 🚧 Estado del Proyecto

🚨 Este sistema **se encuentra actualmente en fase de desarrollo activo**.  
Las funcionalidades se están implementando de forma incremental, y aún faltan módulos clave por integrar.

---

## ✅ Funcionalidades actuales

- 🧑‍💻 Sistema de login y autenticación de usuarios
- 🎛️ Roles: Administrador / Analista (en proceso)
- 📅 Página de inicio con calendario de Google embebido
- 🔐 Control de acceso
- 🔓 Cierre de sesión con mensaje informativo
- 🎨 Interfaz responsive con diseño corporativo
- 📁 Estructura modular lista para escalar a otros módulos (productos, reportes, etc.)

---

## 🗂️ Estructura del proyecto

```
gestionCompras/
├── analysis/              # Scripts de análisis y ETL
├── dashboard/             # App principal (login, inicio, control de acceso)
│   ├── templates/
│   ├── static/
├── web/                   # Configuración general de Django
├── env/                   # Entorno virtual (excluido en .gitignore)
├── manage.py
```

---

## 🚀 Tecnologías utilizadas

- Python 3.13
- Django 5.2
- HTML5, CSS3, JavaScript
- Chart.js (opcional)
- Google Calendar (iframe)
- MySQL (configuración futura)
- Git y GitHub

---

## 🧪 Cómo ejecutar en local

```bash
git clone https://github.com/gestormultifarma/gestion-compras.git
cd gestion-compras
python -m venv env
source env/bin/activate  # o env\Scripts\activate en Windows
pip install -r requirements.txt
python manage.py runserver
```

---

## 🔒 Licencia y uso

Este proyecto es propiedad exclusiva de **Multifarma** y el desarrollador **Alejandro Fernández Herrera**.  
Su uso, modificación o distribución está autorizado únicamente por las partes involucradas.

---

## 📌 Próximos módulos

- 🗃️ Gestión de productos
- 📊 Módulo de reportes y estadísticas
- 📦 Control de inventarios
- 🧾 Generación de solicitudes de compra automatizadas
- 📈 Sincronización con Google Calendar vía API

---

> Desarrollado por Alejandro Fernández Herrera  
> Contacto: **+57 321 464 9277**