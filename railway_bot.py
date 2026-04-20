"""
IN365Bot v2.4 — FIXED
FIXES:
1. YouTube @handle now resolves channel_id via yt page scrape → correct feed URL
2. Instagram uses multiple fallback RSS proxies (rsshub instances + nitter-style)
3. Twitter uses multiple RSSHub public instances with fallback
4. Multi-language system actually translates UI text (10 languages)
5. confirm_url state bug fixed (was storing in "add_source" state, reading from "confirm_url")
6. sent_history unique constraint added to prevent duplicate delivery spam
7. Polling dedup now works correctly for channel subs
8. General stability: DB reconnect on stale connection, better error messages
"""

import os, re, io, asyncio, logging, hashlib, requests, feedparser, psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatMember
from telegram.error import TelegramError
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes, ChatMemberHandler,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BOT_TOKEN           = os.getenv("TELEGRAM_BOT_TOKEN")
DATABASE_URL        = os.getenv("DATABASE_URL")
OWNER_ID            = int(os.getenv("OWNER_ID", "7232714487"))
POLL_INTERVAL       = int(os.getenv("POLL_INTERVAL", "300"))
BOT_USERNAME        = os.getenv("BOT_USERNAME", "in365bot")
MAX_FREE_SOURCES    = 10
INITIAL_FETCH_COUNT = 4
REQUIRED_CHANNEL    = "@copyrightpost"
CHANNEL_INVITE_URL  = "https://t.me/copyrightpost"

if not BOT_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN not set")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not set")

PLATFORMS = {
    "rss":         "📰",
    "youtube":     "▶️",
    "reddit":      "🟠",
    "medium":      "✍️",
    "livejournal": "📝",
    "instagram":   "📸",
    "twitter":     "🐦",
    "telegram":    "✈️",
}

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

TIMEZONES = [
    "UTC","UTC+1","UTC+2","UTC+3","UTC+4","UTC+5","UTC+5:30",
    "UTC+6","UTC+7","UTC+8","UTC+9","UTC+10","UTC+11","UTC+12",
    "UTC-1","UTC-2","UTC-3","UTC-4","UTC-5","UTC-6","UTC-7",
    "UTC-8","UTC-9","UTC-10","UTC-11","UTC-12",
]

LANGUAGES = {
    "en": "🇬🇧 English",
    "ru": "🇷🇺 Russian",
    "de": "🇩🇪 German",
    "fr": "🇫🇷 French",
    "es": "🇪🇸 Spanish",
    "ar": "🇸🇦 Arabic",
    "zh": "🇨🇳 Chinese",
    "hi": "🇮🇳 Hindi",
    "pt": "🇧🇷 Portuguese",
    "bn": "🇧🇩 Bengali",
}

# ── Localized UI strings ──────────────────────────────────────────────────────
STRINGS = {
    "en": {
        "add_source_prompt": "➕ <b>Add source</b>\n\nSend me a URL:",
        "source_added": "✅ <b>Source added!</b>",
        "source_exists": "⚠️ Already subscribed to this source.",
        "source_reactivated": "✅ Source re-activated!",
        "source_removed": "✅ <b>Removed:</b>",
        "source_limit": "❌ Source limit reached ({limit}). Remove one first.",
        "url_not_recognised": "❌ URL not recognised.\nSupported: RSS/Atom, YouTube, Reddit, Medium, Livejournal, Instagram, Twitter, Telegram.",
        "feed_load_error": "❌ Could not load a valid feed from that URL.\n\nTry again in a moment, or use a direct RSS URL.",
        "checking_feed": "🔍 Checking feed, please wait...",
        "fetching_posts": "📥 Fetching initial posts...",
        "fetched_posts": "📤 Fetched <b>{count}</b> initial posts",
        "sources_count": "📊 Sources: {count}/{max}",
        "new_posts_interval": "⚡️ New posts every 5 minutes!",
        "no_sources": "📋 No sources yet (0/{max})",
        "my_feed_header": "📂 <b>You have total {total}/{max} data sources</b>\nPrivate + your channels + your groups subscriptions.",
        "tap_to_manage": "Tap a source to manage it:",
        "settings": "⚙️ <b>Settings</b>",
        "back": "◀️ Back",
        "active": "✅ Active",
        "deactivated": "❌ Deactivated",
        "activate": "✅ Activate data source",
        "deactivate": "❌ Deactivate data source",
        "add_btn": "➕ Add data source",
        "show_media_btn": "📸 Show latest media from this source",
        "remove_btn": "🗑️ Remove data source",
        "confirm_add": "➕ Add data source",
        "confirm_add_media": "➕ Add data source and send latest media",
        "no_posts_found": "📭 No posts found in this feed right now.",
        "sent_posts": "✅ Sent <b>{count}</b> latest posts from <b>{name}</b>.",
        "all_off": "❌ <b>All sources deactivated.</b>",
        "all_on": "✅ <b>All sources activated.</b>",
        "export_caption": "📊 Your data sources exported.",
        "processing": "⏳ Processing, please wait...",
        "session_expired": "❌ Session expired. Please send the URL again.",
        "view_original": "🔗 View original post",
        "join_title": "👋 <b>Welcome to IN365Bot!</b>\n\n⚠️ To use this bot you must first join our channel.\n\n1️⃣ Tap the button below to join <b>@copyrightpost</b>\n2️⃣ Then press <b>✅ I have joined</b>",
        "join_btn": "📢 Join @copyrightpost",
        "joined_btn": "✅ I have joined",
        "not_joined": "⚠️ You haven't joined yet!",
        "banned": "🚫 You have been banned from using this bot.",
        "icons_desc": "Icons description:\n⚡ - fast data extraction\n❌ - data source is deactivated\n⚠️ - marked as not valid\n🔄 - checking data source right now",
        "keywords_saved": "✅ Keywords saved.",
        "keywords_cleared": "✅ <b>Keywords cleared.</b>",
        "cancelled": "❎ Cancelled.",
        "start_use_first": "Use /start first.",
        "already_added": "Already added to your feed.",
        "no_sources_export": "No sources to export.",
        "exported": "✅ Exported!",
        "no_history": "📋 <b>No history yet.</b>",
        "history_header": "📋 <b>Last 20 forwarded posts</b>\n\n",
        "delivery_options": "📩 <b>Delivery options</b>\n\nSilent mode — posts arrive without notification sound.",
        "display_options": "🖥️ <b>Display options</b>",
        "silent_mode": "🤫 Silent mode",
        "hide_link": "🔗 Hide 'View original post' link",
        "filter_header": "🗑️ <b>Keyword filtering</b>\n\nPosts whose title contains a keyword are <b>skipped</b>.\nCurrent: <code>{kw}</code>",
        "set_keywords_prompt": "✏️ <b>Set keywords</b>\n\nSend comma-separated keywords to skip.\nExample: <code>bitcoin, crypto, ads</code>",
        "moderation_header": "🕵️ <b>Moderation mode</b>\n\nWhen ON, posts with potentially inappropriate content are skipped.",
        "butler_header": "🙂 <b>Butler mode</b>\n\nWhen ON, the bot sends a daily digest instead of individual posts.",
        "free_account": "🆓 <b>Free account</b>",
        "forwarded": "📤 Forwarded messages: <b>{count} / 50</b>",
    },
    "ru": {
        "add_source_prompt": "➕ <b>Добавить источник</b>\n\nОтправьте мне URL:",
        "source_added": "✅ <b>Источник добавлен!</b>",
        "source_exists": "⚠️ Вы уже подписаны на этот источник.",
        "source_reactivated": "✅ Источник повторно активирован!",
        "source_removed": "✅ <b>Удалено:</b>",
        "source_limit": "❌ Достигнут лимит источников ({limit}). Сначала удалите один.",
        "url_not_recognised": "❌ URL не распознан.\nПоддерживается: RSS/Atom, YouTube, Reddit, Medium, Livejournal, Instagram, Twitter, Telegram.",
        "feed_load_error": "❌ Не удалось загрузить ленту из этого URL.\n\nПопробуйте позже или используйте прямой RSS URL.",
        "checking_feed": "🔍 Проверяю ленту, подождите...",
        "fetching_posts": "📥 Получаю начальные посты...",
        "fetched_posts": "📤 Получено <b>{count}</b> начальных постов",
        "sources_count": "📊 Источников: {count}/{max}",
        "new_posts_interval": "⚡️ Новые посты каждые 5 минут!",
        "no_sources": "📋 Нет источников (0/{max})",
        "my_feed_header": "📂 <b>Всего {total}/{max} источников данных</b>\nЛичные + ваши каналы + группы.",
        "tap_to_manage": "Нажмите на источник для управления:",
        "settings": "⚙️ <b>Настройки</b>",
        "back": "◀️ Назад",
        "active": "✅ Активен",
        "deactivated": "❌ Деактивирован",
        "activate": "✅ Активировать источник",
        "deactivate": "❌ Деактивировать источник",
        "add_btn": "➕ Добавить источник данных",
        "show_media_btn": "📸 Показать последние медиа из этого источника",
        "remove_btn": "🗑️ Удалить источник данных",
        "confirm_add": "➕ Добавить источник данных",
        "confirm_add_media": "➕ Добавить источник и отправить медиа",
        "no_posts_found": "📭 Посты в этой ленте не найдены.",
        "sent_posts": "✅ Отправлено <b>{count}</b> последних постов из <b>{name}</b>.",
        "all_off": "❌ <b>Все источники деактивированы.</b>",
        "all_on": "✅ <b>Все источники активированы.</b>",
        "export_caption": "📊 Ваши источники данных экспортированы.",
        "processing": "⏳ Обработка, подождите...",
        "session_expired": "❌ Сессия истекла. Пожалуйста, отправьте URL снова.",
        "view_original": "🔗 Просмотреть оригинал",
        "join_title": "👋 <b>Добро пожаловать в IN365Bot!</b>\n\n⚠️ Для использования бота вы должны сначала вступить в наш канал.\n\n1️⃣ Нажмите кнопку ниже, чтобы вступить в <b>@copyrightpost</b>\n2️⃣ Затем нажмите <b>✅ Я вступил</b>",
        "join_btn": "📢 Вступить в @copyrightpost",
        "joined_btn": "✅ Я вступил",
        "not_joined": "⚠️ Вы ещё не вступили!",
        "banned": "🚫 Вы заблокированы.",
        "icons_desc": "Описание иконок:\n⚡ - быстрое извлечение данных\n❌ - источник деактивирован\n⚠️ - помечен как недействительный\n🔄 - проверка источника сейчас",
        "keywords_saved": "✅ Ключевые слова сохранены.",
        "keywords_cleared": "✅ <b>Ключевые слова очищены.</b>",
        "cancelled": "❎ Отменено.",
        "start_use_first": "Используйте /start сначала.",
        "already_added": "Уже добавлено в вашу ленту.",
        "no_sources_export": "Нет источников для экспорта.",
        "exported": "✅ Экспортировано!",
        "no_history": "📋 <b>Истории нет.</b>",
        "history_header": "📋 <b>Последние 20 пересланных постов</b>\n\n",
        "delivery_options": "📩 <b>Параметры доставки</b>\n\nБесшумный режим — посты приходят без звука уведомления.",
        "display_options": "🖥️ <b>Параметры отображения</b>",
        "silent_mode": "🤫 Бесшумный режим",
        "hide_link": "🔗 Скрыть ссылку 'Просмотреть оригинал'",
        "filter_header": "🗑️ <b>Фильтрация по ключевым словам</b>\n\nПосты, содержащие ключевое слово в заголовке, <b>пропускаются</b>.\nТекущие: <code>{kw}</code>",
        "set_keywords_prompt": "✏️ <b>Установить ключевые слова</b>\n\nОтправьте слова через запятую для пропуска.\nПример: <code>биткоин, крипто, реклама</code>",
        "moderation_header": "🕵️ <b>Режим модерации</b>\n\nКогда ВКЛЮЧЁН, посты с неприемлемым контентом пропускаются.",
        "butler_header": "🙂 <b>Режим дворецкого</b>\n\nКогда ВКЛЮЧЁН, бот отправляет ежедневный дайджест.",
        "free_account": "🆓 <b>Бесплатный аккаунт</b>",
        "forwarded": "📤 Пересланных сообщений: <b>{count} / 50</b>",
    },
    "de": {
        "add_source_prompt": "➕ <b>Quelle hinzufügen</b>\n\nSende mir eine URL:",
        "source_added": "✅ <b>Quelle hinzugefügt!</b>",
        "source_exists": "⚠️ Du hast diese Quelle bereits abonniert.",
        "source_reactivated": "✅ Quelle reaktiviert!",
        "source_removed": "✅ <b>Entfernt:</b>",
        "source_limit": "❌ Quellenlimit erreicht ({limit}). Zuerst eine entfernen.",
        "url_not_recognised": "❌ URL nicht erkannt.\nUnterstützt: RSS/Atom, YouTube, Reddit, Medium, Livejournal, Instagram, Twitter, Telegram.",
        "feed_load_error": "❌ Feed konnte nicht geladen werden.\n\nBitte versuche es später erneut.",
        "checking_feed": "🔍 Feed wird überprüft, bitte warten...",
        "fetching_posts": "📥 Erste Beiträge werden geladen...",
        "fetched_posts": "📤 <b>{count}</b> erste Beiträge geladen",
        "sources_count": "📊 Quellen: {count}/{max}",
        "new_posts_interval": "⚡️ Neue Beiträge alle 5 Minuten!",
        "no_sources": "📋 Noch keine Quellen (0/{max})",
        "my_feed_header": "📂 <b>Insgesamt {total}/{max} Datenquellen</b>\nPersönlich + deine Kanäle + Gruppen.",
        "tap_to_manage": "Tippe auf eine Quelle zum Verwalten:",
        "settings": "⚙️ <b>Einstellungen</b>",
        "back": "◀️ Zurück",
        "active": "✅ Aktiv",
        "deactivated": "❌ Deaktiviert",
        "activate": "✅ Datenquelle aktivieren",
        "deactivate": "❌ Datenquelle deaktivieren",
        "add_btn": "➕ Datenquelle hinzufügen",
        "show_media_btn": "📸 Neueste Medien dieser Quelle anzeigen",
        "remove_btn": "🗑️ Datenquelle entfernen",
        "confirm_add": "➕ Datenquelle hinzufügen",
        "confirm_add_media": "➕ Quelle hinzufügen und Medien senden",
        "no_posts_found": "📭 Keine Beiträge in diesem Feed gefunden.",
        "sent_posts": "✅ <b>{count}</b> neueste Beiträge von <b>{name}</b> gesendet.",
        "all_off": "❌ <b>Alle Quellen deaktiviert.</b>",
        "all_on": "✅ <b>Alle Quellen aktiviert.</b>",
        "export_caption": "📊 Deine Datenquellen wurden exportiert.",
        "processing": "⏳ Verarbeitung, bitte warten...",
        "session_expired": "❌ Sitzung abgelaufen. Bitte URL erneut senden.",
        "view_original": "🔗 Original ansehen",
        "join_title": "👋 <b>Willkommen bei IN365Bot!</b>\n\n⚠️ Um den Bot zu nutzen, musst du zuerst unserem Kanal beitreten.\n\n1️⃣ Tippe auf den Button, um <b>@copyrightpost</b> beizutreten\n2️⃣ Dann drücke <b>✅ Ich bin beigetreten</b>",
        "join_btn": "📢 @copyrightpost beitreten",
        "joined_btn": "✅ Ich bin beigetreten",
        "not_joined": "⚠️ Du bist noch nicht beigetreten!",
        "banned": "🚫 Du wurdest gesperrt.",
        "icons_desc": "Icon-Beschreibung:\n⚡ - schnelle Datenextraktion\n❌ - Quelle deaktiviert\n⚠️ - als ungültig markiert\n🔄 - Quelle wird gerade geprüft",
        "keywords_saved": "✅ Schlüsselwörter gespeichert.",
        "keywords_cleared": "✅ <b>Schlüsselwörter gelöscht.</b>",
        "cancelled": "❎ Abgebrochen.",
        "start_use_first": "Verwende zuerst /start.",
        "already_added": "Bereits zu deinem Feed hinzugefügt.",
        "no_sources_export": "Keine Quellen zum Exportieren.",
        "exported": "✅ Exportiert!",
        "no_history": "📋 <b>Noch keine Historie.</b>",
        "history_header": "📋 <b>Letzte 20 weitergeleitete Beiträge</b>\n\n",
        "delivery_options": "📩 <b>Lieferoptionen</b>\n\nStiller Modus — Beiträge kommen ohne Benachrichtigungston.",
        "display_options": "🖥️ <b>Anzeigeoptionen</b>",
        "silent_mode": "🤫 Stiller Modus",
        "hide_link": "🔗 'Original ansehen'-Link verstecken",
        "filter_header": "🗑️ <b>Schlüsselwort-Filterung</b>\n\nBeiträge mit einem Schlüsselwort im Titel werden <b>übersprungen</b>.\nAktuell: <code>{kw}</code>",
        "set_keywords_prompt": "✏️ <b>Schlüsselwörter setzen</b>\n\nSende kommagetrennte Schlüsselwörter zum Überspringen.\nBeispiel: <code>bitcoin, krypto, werbung</code>",
        "moderation_header": "🕵️ <b>Moderationsmodus</b>\n\nWenn EIN, werden Beiträge mit unangemessenen Inhalten übersprungen.",
        "butler_header": "🙂 <b>Butler-Modus</b>\n\nWenn EIN, sendet der Bot eine tägliche Zusammenfassung.",
        "free_account": "🆓 <b>Kostenloses Konto</b>",
        "forwarded": "📤 Weitergeleitete Nachrichten: <b>{count} / 50</b>",
    },
    "fr": {
        "add_source_prompt": "➕ <b>Ajouter une source</b>\n\nEnvoyez-moi une URL :",
        "source_added": "✅ <b>Source ajoutée !</b>",
        "source_exists": "⚠️ Vous êtes déjà abonné à cette source.",
        "source_reactivated": "✅ Source réactivée !",
        "source_removed": "✅ <b>Supprimé :</b>",
        "source_limit": "❌ Limite de sources atteinte ({limit}). Supprimez-en une d'abord.",
        "url_not_recognised": "❌ URL non reconnue.\nPris en charge : RSS/Atom, YouTube, Reddit, Medium, Livejournal, Instagram, Twitter, Telegram.",
        "feed_load_error": "❌ Impossible de charger le flux depuis cette URL.\n\nRéessayez dans un moment.",
        "checking_feed": "🔍 Vérification du flux, veuillez patienter...",
        "fetching_posts": "📥 Récupération des premiers articles...",
        "fetched_posts": "📤 <b>{count}</b> premiers articles récupérés",
        "sources_count": "📊 Sources : {count}/{max}",
        "new_posts_interval": "⚡️ Nouveaux articles toutes les 5 minutes !",
        "no_sources": "📋 Aucune source pour l'instant (0/{max})",
        "my_feed_header": "📂 <b>Total {total}/{max} sources de données</b>\nPersonnel + vos chaînes + groupes.",
        "tap_to_manage": "Appuyez sur une source pour la gérer :",
        "settings": "⚙️ <b>Paramètres</b>",
        "back": "◀️ Retour",
        "active": "✅ Actif",
        "deactivated": "❌ Désactivé",
        "activate": "✅ Activer la source",
        "deactivate": "❌ Désactiver la source",
        "add_btn": "➕ Ajouter une source de données",
        "show_media_btn": "📸 Afficher les derniers médias de cette source",
        "remove_btn": "🗑️ Supprimer la source de données",
        "confirm_add": "➕ Ajouter la source de données",
        "confirm_add_media": "➕ Ajouter la source et envoyer les médias",
        "no_posts_found": "📭 Aucun article trouvé dans ce flux.",
        "sent_posts": "✅ <b>{count}</b> derniers articles de <b>{name}</b> envoyés.",
        "all_off": "❌ <b>Toutes les sources désactivées.</b>",
        "all_on": "✅ <b>Toutes les sources activées.</b>",
        "export_caption": "📊 Vos sources de données exportées.",
        "processing": "⏳ Traitement en cours, veuillez patienter...",
        "session_expired": "❌ Session expirée. Veuillez renvoyer l'URL.",
        "view_original": "🔗 Voir l'article original",
        "join_title": "👋 <b>Bienvenue sur IN365Bot !</b>\n\n⚠️ Pour utiliser ce bot, vous devez d'abord rejoindre notre chaîne.\n\n1️⃣ Appuyez sur le bouton ci-dessous pour rejoindre <b>@copyrightpost</b>\n2️⃣ Puis appuyez sur <b>✅ J'ai rejoint</b>",
        "join_btn": "📢 Rejoindre @copyrightpost",
        "joined_btn": "✅ J'ai rejoint",
        "not_joined": "⚠️ Vous n'avez pas encore rejoint !",
        "banned": "🚫 Vous avez été banni.",
        "icons_desc": "Description des icônes :\n⚡ - extraction rapide\n❌ - source désactivée\n⚠️ - marqué comme invalide\n🔄 - vérification en cours",
        "keywords_saved": "✅ Mots-clés enregistrés.",
        "keywords_cleared": "✅ <b>Mots-clés effacés.</b>",
        "cancelled": "❎ Annulé.",
        "start_use_first": "Utilisez /start d'abord.",
        "already_added": "Déjà ajouté à votre flux.",
        "no_sources_export": "Aucune source à exporter.",
        "exported": "✅ Exporté !",
        "no_history": "📋 <b>Pas encore d'historique.</b>",
        "history_header": "📋 <b>20 derniers articles transmis</b>\n\n",
        "delivery_options": "📩 <b>Options de livraison</b>\n\nMode silencieux — les articles arrivent sans son de notification.",
        "display_options": "🖥️ <b>Options d'affichage</b>",
        "silent_mode": "🤫 Mode silencieux",
        "hide_link": "🔗 Masquer le lien 'Voir l'original'",
        "filter_header": "🗑️ <b>Filtrage par mots-clés</b>\n\nLes articles dont le titre contient un mot-clé sont <b>ignorés</b>.\nActuel : <code>{kw}</code>",
        "set_keywords_prompt": "✏️ <b>Définir des mots-clés</b>\n\nEnvoyez des mots-clés séparés par des virgules à ignorer.\nExemple : <code>bitcoin, crypto, pub</code>",
        "moderation_header": "🕵️ <b>Mode modération</b>\n\nQuand ACTIVÉ, les articles inappropriés sont ignorés.",
        "butler_header": "🙂 <b>Mode majordome</b>\n\nQuand ACTIVÉ, le bot envoie un résumé quotidien.",
        "free_account": "🆓 <b>Compte gratuit</b>",
        "forwarded": "📤 Messages transmis : <b>{count} / 50</b>",
    },
    "es": {
        "add_source_prompt": "➕ <b>Agregar fuente</b>\n\nEnvíame una URL:",
        "source_added": "✅ <b>¡Fuente agregada!</b>",
        "source_exists": "⚠️ Ya estás suscrito a esta fuente.",
        "source_reactivated": "✅ ¡Fuente reactivada!",
        "source_removed": "✅ <b>Eliminado:</b>",
        "source_limit": "❌ Límite de fuentes alcanzado ({limit}). Elimina una primero.",
        "url_not_recognised": "❌ URL no reconocida.\nCompatible: RSS/Atom, YouTube, Reddit, Medium, Livejournal, Instagram, Twitter, Telegram.",
        "feed_load_error": "❌ No se pudo cargar el feed desde esa URL.\n\nInténtalo de nuevo en un momento.",
        "checking_feed": "🔍 Verificando feed, por favor espera...",
        "fetching_posts": "📥 Obteniendo publicaciones iniciales...",
        "fetched_posts": "📤 <b>{count}</b> publicaciones iniciales obtenidas",
        "sources_count": "📊 Fuentes: {count}/{max}",
        "new_posts_interval": "⚡️ ¡Nuevas publicaciones cada 5 minutos!",
        "no_sources": "📋 Sin fuentes aún (0/{max})",
        "my_feed_header": "📂 <b>Total {total}/{max} fuentes de datos</b>\nPersonal + tus canales + grupos.",
        "tap_to_manage": "Toca una fuente para gestionarla:",
        "settings": "⚙️ <b>Configuración</b>",
        "back": "◀️ Atrás",
        "active": "✅ Activo",
        "deactivated": "❌ Desactivado",
        "activate": "✅ Activar fuente de datos",
        "deactivate": "❌ Desactivar fuente de datos",
        "add_btn": "➕ Agregar fuente de datos",
        "show_media_btn": "📸 Mostrar últimos medios de esta fuente",
        "remove_btn": "🗑️ Eliminar fuente de datos",
        "confirm_add": "➕ Agregar fuente de datos",
        "confirm_add_media": "➕ Agregar fuente y enviar medios",
        "no_posts_found": "📭 No se encontraron publicaciones en este feed.",
        "sent_posts": "✅ <b>{count}</b> últimas publicaciones de <b>{name}</b> enviadas.",
        "all_off": "❌ <b>Todas las fuentes desactivadas.</b>",
        "all_on": "✅ <b>Todas las fuentes activadas.</b>",
        "export_caption": "📊 Tus fuentes de datos exportadas.",
        "processing": "⏳ Procesando, por favor espera...",
        "session_expired": "❌ Sesión expirada. Por favor envía la URL de nuevo.",
        "view_original": "🔗 Ver publicación original",
        "join_title": "👋 <b>¡Bienvenido a IN365Bot!</b>\n\n⚠️ Para usar este bot debes unirte a nuestro canal.\n\n1️⃣ Toca el botón para unirte a <b>@copyrightpost</b>\n2️⃣ Luego presiona <b>✅ Me uní</b>",
        "join_btn": "📢 Unirse a @copyrightpost",
        "joined_btn": "✅ Me uní",
        "not_joined": "⚠️ ¡Aún no te has unido!",
        "banned": "🚫 Has sido baneado.",
        "icons_desc": "Descripción de iconos:\n⚡ - extracción rápida\n❌ - fuente desactivada\n⚠️ - marcado como inválido\n🔄 - verificando fuente ahora",
        "keywords_saved": "✅ Palabras clave guardadas.",
        "keywords_cleared": "✅ <b>Palabras clave eliminadas.</b>",
        "cancelled": "❎ Cancelado.",
        "start_use_first": "Usa /start primero.",
        "already_added": "Ya agregado a tu feed.",
        "no_sources_export": "No hay fuentes para exportar.",
        "exported": "✅ ¡Exportado!",
        "no_history": "📋 <b>Sin historial aún.</b>",
        "history_header": "📋 <b>Últimas 20 publicaciones reenviadas</b>\n\n",
        "delivery_options": "📩 <b>Opciones de entrega</b>\n\nModo silencioso — las publicaciones llegan sin sonido.",
        "display_options": "🖥️ <b>Opciones de visualización</b>",
        "silent_mode": "🤫 Modo silencioso",
        "hide_link": "🔗 Ocultar enlace 'Ver original'",
        "filter_header": "🗑️ <b>Filtrado por palabras clave</b>\n\nLas publicaciones con una palabra clave en el título son <b>omitidas</b>.\nActual: <code>{kw}</code>",
        "set_keywords_prompt": "✏️ <b>Establecer palabras clave</b>\n\nEnvía palabras clave separadas por comas para omitir.\nEjemplo: <code>bitcoin, cripto, anuncios</code>",
        "moderation_header": "🕵️ <b>Modo moderación</b>\n\nCuando está ACTIVADO, las publicaciones inapropiadas se omiten.",
        "butler_header": "🙂 <b>Modo mayordomo</b>\n\nCuando está ACTIVADO, el bot envía un resumen diario.",
        "free_account": "🆓 <b>Cuenta gratuita</b>",
        "forwarded": "📤 Mensajes reenviados: <b>{count} / 50</b>",
    },
    "ar": {
        "add_source_prompt": "➕ <b>إضافة مصدر</b>\n\nأرسل لي رابط URL:",
        "source_added": "✅ <b>تمت إضافة المصدر!</b>",
        "source_exists": "⚠️ أنت مشترك بالفعل في هذا المصدر.",
        "source_reactivated": "✅ تم إعادة تفعيل المصدر!",
        "source_removed": "✅ <b>تمت الإزالة:</b>",
        "source_limit": "❌ تم الوصول إلى حد المصادر ({limit}). احذف مصدراً أولاً.",
        "url_not_recognised": "❌ الرابط غير معروف.\nمدعوم: RSS/Atom, YouTube, Reddit, Medium, Livejournal, Instagram, Twitter, Telegram.",
        "feed_load_error": "❌ تعذر تحميل الخلاصة من هذا الرابط.\n\nحاول مرة أخرى بعد قليل.",
        "checking_feed": "🔍 جارٍ التحقق من الخلاصة، يرجى الانتظار...",
        "fetching_posts": "📥 جارٍ جلب المنشورات الأولية...",
        "fetched_posts": "📤 تم جلب <b>{count}</b> منشورات أولية",
        "sources_count": "📊 المصادر: {count}/{max}",
        "new_posts_interval": "⚡️ منشورات جديدة كل 5 دقائق!",
        "no_sources": "📋 لا توجد مصادر بعد (0/{max})",
        "my_feed_header": "📂 <b>إجمالي {total}/{max} مصادر البيانات</b>\nشخصي + قنواتك + المجموعات.",
        "tap_to_manage": "اضغط على مصدر لإدارته:",
        "settings": "⚙️ <b>الإعدادات</b>",
        "back": "◀️ رجوع",
        "active": "✅ نشط",
        "deactivated": "❌ معطل",
        "activate": "✅ تفعيل مصدر البيانات",
        "deactivate": "❌ تعطيل مصدر البيانات",
        "add_btn": "➕ إضافة مصدر بيانات",
        "show_media_btn": "📸 عرض أحدث الوسائط من هذا المصدر",
        "remove_btn": "🗑️ إزالة مصدر البيانات",
        "confirm_add": "➕ إضافة مصدر البيانات",
        "confirm_add_media": "➕ إضافة مصدر وإرسال الوسائط",
        "no_posts_found": "📭 لم يتم العثور على منشورات في هذه الخلاصة.",
        "sent_posts": "✅ تم إرسال <b>{count}</b> منشورات أخيرة من <b>{name}</b>.",
        "all_off": "❌ <b>تم تعطيل جميع المصادر.</b>",
        "all_on": "✅ <b>تم تفعيل جميع المصادر.</b>",
        "export_caption": "📊 تم تصدير مصادر بياناتك.",
        "processing": "⏳ جارٍ المعالجة، يرجى الانتظار...",
        "session_expired": "❌ انتهت الجلسة. يرجى إرسال الرابط مرة أخرى.",
        "view_original": "🔗 عرض المنشور الأصلي",
        "join_title": "👋 <b>مرحباً بك في IN365Bot!</b>\n\n⚠️ لاستخدام هذا البوت يجب أن تنضم إلى قناتنا أولاً.\n\n1️⃣ اضغط على الزر للانضمام إلى <b>@copyrightpost</b>\n2️⃣ ثم اضغط <b>✅ انضممت</b>",
        "join_btn": "📢 انضم إلى @copyrightpost",
        "joined_btn": "✅ انضممت",
        "not_joined": "⚠️ لم تنضم بعد!",
        "banned": "🚫 تم حظرك.",
        "icons_desc": "وصف الأيقونات:\n⚡ - استخراج سريع\n❌ - المصدر معطل\n⚠️ - مُعلَّم كغير صالح\n🔄 - يجري التحقق الآن",
        "keywords_saved": "✅ تم حفظ الكلمات المفتاحية.",
        "keywords_cleared": "✅ <b>تم مسح الكلمات المفتاحية.</b>",
        "cancelled": "❎ تم الإلغاء.",
        "start_use_first": "استخدم /start أولاً.",
        "already_added": "تمت إضافته بالفعل إلى خلاصتك.",
        "no_sources_export": "لا توجد مصادر للتصدير.",
        "exported": "✅ تم التصدير!",
        "no_history": "📋 <b>لا يوجد سجل بعد.</b>",
        "history_header": "📋 <b>آخر 20 منشوراً تم إعادة توجيهه</b>\n\n",
        "delivery_options": "📩 <b>خيارات التسليم</b>\n\nالوضع الصامت — تصل المنشورات بدون صوت إشعار.",
        "display_options": "🖥️ <b>خيارات العرض</b>",
        "silent_mode": "🤫 الوضع الصامت",
        "hide_link": "🔗 إخفاء رابط 'عرض الأصل'",
        "filter_header": "🗑️ <b>التصفية بالكلمات المفتاحية</b>\n\nالمنشورات التي تحتوي كلمة مفتاحية في العنوان <b>تُتخطى</b>.\nالحالية: <code>{kw}</code>",
        "set_keywords_prompt": "✏️ <b>تعيين الكلمات المفتاحية</b>\n\nأرسل كلمات مفتاحية مفصولة بفواصل للتخطي.\nمثال: <code>بيتكوين، كريبتو، إعلانات</code>",
        "moderation_header": "🕵️ <b>وضع الإشراف</b>\n\nعند التشغيل، تُتخطى المنشورات غير اللائقة.",
        "butler_header": "🙂 <b>وضع الخادم</b>\n\nعند التشغيل، يرسل البوت ملخصاً يومياً.",
        "free_account": "🆓 <b>حساب مجاني</b>",
        "forwarded": "📤 الرسائل المُعاد توجيهها: <b>{count} / 50</b>",
    },
    "zh": {
        "add_source_prompt": "➕ <b>添加来源</b>\n\n请发送一个URL：",
        "source_added": "✅ <b>来源已添加！</b>",
        "source_exists": "⚠️ 您已订阅此来源。",
        "source_reactivated": "✅ 来源已重新激活！",
        "source_removed": "✅ <b>已删除：</b>",
        "source_limit": "❌ 已达到来源限制（{limit}）。请先删除一个。",
        "url_not_recognised": "❌ 未识别的URL。\n支持：RSS/Atom、YouTube、Reddit、Medium、Livejournal、Instagram、Twitter、Telegram。",
        "feed_load_error": "❌ 无法从该URL加载有效的Feed。\n\n请稍后再试。",
        "checking_feed": "🔍 正在检查Feed，请稍候...",
        "fetching_posts": "📥 正在获取初始帖子...",
        "fetched_posts": "📤 已获取 <b>{count}</b> 条初始帖子",
        "sources_count": "📊 来源：{count}/{max}",
        "new_posts_interval": "⚡️ 每5分钟更新新帖子！",
        "no_sources": "📋 暂无来源（0/{max}）",
        "my_feed_header": "📂 <b>共 {total}/{max} 个数据来源</b>\n个人 + 您的频道 + 群组。",
        "tap_to_manage": "点击来源进行管理：",
        "settings": "⚙️ <b>设置</b>",
        "back": "◀️ 返回",
        "active": "✅ 活跃",
        "deactivated": "❌ 已停用",
        "activate": "✅ 激活数据来源",
        "deactivate": "❌ 停用数据来源",
        "add_btn": "➕ 添加数据来源",
        "show_media_btn": "📸 显示此来源的最新媒体",
        "remove_btn": "🗑️ 删除数据来源",
        "confirm_add": "➕ 添加数据来源",
        "confirm_add_media": "➕ 添加来源并发送媒体",
        "no_posts_found": "📭 此Feed中暂无帖子。",
        "sent_posts": "✅ 已发送 <b>{name}</b> 的 <b>{count}</b> 条最新帖子。",
        "all_off": "❌ <b>所有来源已停用。</b>",
        "all_on": "✅ <b>所有来源已激活。</b>",
        "export_caption": "📊 您的数据来源已导出。",
        "processing": "⏳ 处理中，请稍候...",
        "session_expired": "❌ 会话已过期。请重新发送URL。",
        "view_original": "🔗 查看原帖",
        "join_title": "👋 <b>欢迎使用IN365Bot！</b>\n\n⚠️ 要使用此机器人，您必须先加入我们的频道。\n\n1️⃣ 点击下方按钮加入 <b>@copyrightpost</b>\n2️⃣ 然后按 <b>✅ 我已加入</b>",
        "join_btn": "📢 加入 @copyrightpost",
        "joined_btn": "✅ 我已加入",
        "not_joined": "⚠️ 您还未加入！",
        "banned": "🚫 您已被封禁。",
        "icons_desc": "图标说明：\n⚡ - 快速数据提取\n❌ - 来源已停用\n⚠️ - 标记为无效\n🔄 - 正在检查来源",
        "keywords_saved": "✅ 关键词已保存。",
        "keywords_cleared": "✅ <b>关键词已清除。</b>",
        "cancelled": "❎ 已取消。",
        "start_use_first": "请先使用 /start。",
        "already_added": "已添加到您的Feed。",
        "no_sources_export": "没有可导出的来源。",
        "exported": "✅ 已导出！",
        "no_history": "📋 <b>暂无历史记录。</b>",
        "history_header": "📋 <b>最近20条转发帖子</b>\n\n",
        "delivery_options": "📩 <b>投递选项</b>\n\n静音模式 — 帖子到达时不发出通知声。",
        "display_options": "🖥️ <b>显示选项</b>",
        "silent_mode": "🤫 静音模式",
        "hide_link": "🔗 隐藏'查看原帖'链接",
        "filter_header": "🗑️ <b>关键词过滤</b>\n\n标题中包含关键词的帖子将被<b>跳过</b>。\n当前：<code>{kw}</code>",
        "set_keywords_prompt": "✏️ <b>设置关键词</b>\n\n发送用逗号分隔的关键词以跳过。\n示例：<code>比特币, 加密货币, 广告</code>",
        "moderation_header": "🕵️ <b>审核模式</b>\n\n开启时，含不当内容的帖子将被跳过。",
        "butler_header": "🙂 <b>管家模式</b>\n\n开启时，机器人每天发送摘要。",
        "free_account": "🆓 <b>免费账户</b>",
        "forwarded": "📤 已转发消息：<b>{count} / 50</b>",
    },
    "hi": {
        "add_source_prompt": "➕ <b>स्रोत जोड़ें</b>\n\nमुझे एक URL भेजें:",
        "source_added": "✅ <b>स्रोत जोड़ा गया!</b>",
        "source_exists": "⚠️ आप पहले से इस स्रोत की सदस्यता ले चुके हैं।",
        "source_reactivated": "✅ स्रोत पुनः सक्रिय हुआ!",
        "source_removed": "✅ <b>हटाया गया:</b>",
        "source_limit": "❌ स्रोत सीमा पहुंच गई ({limit})। पहले एक हटाएं।",
        "url_not_recognised": "❌ URL पहचाना नहीं गया।\nसमर्थित: RSS/Atom, YouTube, Reddit, Medium, Livejournal, Instagram, Twitter, Telegram।",
        "feed_load_error": "❌ उस URL से फ़ीड लोड नहीं हो सकी।\n\nकुछ देर बाद पुनः प्रयास करें।",
        "checking_feed": "🔍 फ़ीड जांची जा रही है, कृपया प्रतीक्षा करें...",
        "fetching_posts": "📥 प्रारंभिक पोस्ट प्राप्त हो रहे हैं...",
        "fetched_posts": "📤 <b>{count}</b> प्रारंभिक पोस्ट प्राप्त",
        "sources_count": "📊 स्रोत: {count}/{max}",
        "new_posts_interval": "⚡️ हर 5 मिनट में नई पोस्ट!",
        "no_sources": "📋 अभी कोई स्रोत नहीं (0/{max})",
        "my_feed_header": "📂 <b>कुल {total}/{max} डेटा स्रोत</b>\nव्यक्तिगत + आपके चैनल + समूह।",
        "tap_to_manage": "प्रबंधित करने के लिए किसी स्रोत पर टैप करें:",
        "settings": "⚙️ <b>सेटिंग्स</b>",
        "back": "◀️ वापस",
        "active": "✅ सक्रिय",
        "deactivated": "❌ निष्क्रिय",
        "activate": "✅ डेटा स्रोत सक्रिय करें",
        "deactivate": "❌ डेटा स्रोत निष्क्रिय करें",
        "add_btn": "➕ डेटा स्रोत जोड़ें",
        "show_media_btn": "📸 इस स्रोत से नवीनतम मीडिया दिखाएं",
        "remove_btn": "🗑️ डेटा स्रोत हटाएं",
        "confirm_add": "➕ डेटा स्रोत जोड़ें",
        "confirm_add_media": "➕ स्रोत जोड़ें और मीडिया भेजें",
        "no_posts_found": "📭 इस फ़ीड में कोई पोस्ट नहीं मिली।",
        "sent_posts": "✅ <b>{name}</b> से <b>{count}</b> नवीनतम पोस्ट भेजी गई।",
        "all_off": "❌ <b>सभी स्रोत निष्क्रिय।</b>",
        "all_on": "✅ <b>सभी स्रोत सक्रिय।</b>",
        "export_caption": "📊 आपके डेटा स्रोत निर्यात किए गए।",
        "processing": "⏳ प्रक्रिया हो रही है, कृपया प्रतीक्षा करें...",
        "session_expired": "❌ सत्र समाप्त। कृपया URL फिर से भेजें।",
        "view_original": "🔗 मूल पोस्ट देखें",
        "join_title": "👋 <b>IN365Bot में आपका स्वागत है!</b>\n\n⚠️ इस बोट का उपयोग करने के लिए पहले हमारे चैनल से जुड़ें।\n\n1️⃣ <b>@copyrightpost</b> से जुड़ने के लिए नीचे बटन दबाएं\n2️⃣ फिर <b>✅ मैं जुड़ गया</b> दबाएं",
        "join_btn": "📢 @copyrightpost से जुड़ें",
        "joined_btn": "✅ मैं जुड़ गया",
        "not_joined": "⚠️ आप अभी तक नहीं जुड़े!",
        "banned": "🚫 आपको प्रतिबंधित किया गया है।",
        "icons_desc": "आइकन विवरण:\n⚡ - तेज़ डेटा निष्कर्षण\n❌ - स्रोत निष्क्रिय\n⚠️ - अमान्य के रूप में चिह्नित\n🔄 - अभी स्रोत जांचा जा रहा है",
        "keywords_saved": "✅ कीवर्ड सहेजे गए।",
        "keywords_cleared": "✅ <b>कीवर्ड साफ़ किए गए।</b>",
        "cancelled": "❎ रद्द किया गया।",
        "start_use_first": "पहले /start का उपयोग करें।",
        "already_added": "पहले से आपके फ़ीड में जोड़ा गया।",
        "no_sources_export": "निर्यात के लिए कोई स्रोत नहीं।",
        "exported": "✅ निर्यात हुआ!",
        "no_history": "📋 <b>अभी तक कोई इतिहास नहीं।</b>",
        "history_header": "📋 <b>अंतिम 20 अग्रेषित पोस्ट</b>\n\n",
        "delivery_options": "📩 <b>डिलीवरी विकल्प</b>\n\nसाइलेंट मोड — पोस्ट बिना नोटिफ़िकेशन ध्वनि के आती हैं।",
        "display_options": "🖥️ <b>प्रदर्शन विकल्प</b>",
        "silent_mode": "🤫 साइलेंट मोड",
        "hide_link": "🔗 'मूल देखें' लिंक छुपाएं",
        "filter_header": "🗑️ <b>कीवर्ड फ़िल्टरिंग</b>\n\nशीर्षक में कीवर्ड वाली पोस्ट <b>छोड़ी जाती हैं</b>।\nवर्तमान: <code>{kw}</code>",
        "set_keywords_prompt": "✏️ <b>कीवर्ड सेट करें</b>\n\nछोड़ने के लिए कॉमा से अलग कीवर्ड भेजें।\nउदाहरण: <code>बिटकॉइन, क्रिप्टो, विज्ञापन</code>",
        "moderation_header": "🕵️ <b>मॉडरेशन मोड</b>\n\nजब चालू हो, अनुचित सामग्री वाली पोस्ट छोड़ी जाती हैं।",
        "butler_header": "🙂 <b>बटलर मोड</b>\n\nजब चालू हो, बोट दैनिक डाइजेस्ट भेजता है।",
        "free_account": "🆓 <b>मुफ़्त खाता</b>",
        "forwarded": "📤 अग्रेषित संदेश: <b>{count} / 50</b>",
    },
    "pt": {
        "add_source_prompt": "➕ <b>Adicionar fonte</b>\n\nEnvie-me um URL:",
        "source_added": "✅ <b>Fonte adicionada!</b>",
        "source_exists": "⚠️ Você já está inscrito nesta fonte.",
        "source_reactivated": "✅ Fonte reativada!",
        "source_removed": "✅ <b>Removido:</b>",
        "source_limit": "❌ Limite de fontes atingido ({limit}). Remova uma primeiro.",
        "url_not_recognised": "❌ URL não reconhecido.\nSuportado: RSS/Atom, YouTube, Reddit, Medium, Livejournal, Instagram, Twitter, Telegram.",
        "feed_load_error": "❌ Não foi possível carregar o feed desse URL.\n\nTente novamente em instantes.",
        "checking_feed": "🔍 Verificando feed, aguarde...",
        "fetching_posts": "📥 Buscando posts iniciais...",
        "fetched_posts": "📤 <b>{count}</b> posts iniciais obtidos",
        "sources_count": "📊 Fontes: {count}/{max}",
        "new_posts_interval": "⚡️ Novos posts a cada 5 minutos!",
        "no_sources": "📋 Nenhuma fonte ainda (0/{max})",
        "my_feed_header": "📂 <b>Total de {total}/{max} fontes de dados</b>\nPessoal + seus canais + grupos.",
        "tap_to_manage": "Toque numa fonte para gerenciá-la:",
        "settings": "⚙️ <b>Configurações</b>",
        "back": "◀️ Voltar",
        "active": "✅ Ativo",
        "deactivated": "❌ Desativado",
        "activate": "✅ Ativar fonte de dados",
        "deactivate": "❌ Desativar fonte de dados",
        "add_btn": "➕ Adicionar fonte de dados",
        "show_media_btn": "📸 Mostrar mídia mais recente desta fonte",
        "remove_btn": "🗑️ Remover fonte de dados",
        "confirm_add": "➕ Adicionar fonte de dados",
        "confirm_add_media": "➕ Adicionar fonte e enviar mídia",
        "no_posts_found": "📭 Nenhum post encontrado neste feed.",
        "sent_posts": "✅ <b>{count}</b> posts mais recentes de <b>{name}</b> enviados.",
        "all_off": "❌ <b>Todas as fontes desativadas.</b>",
        "all_on": "✅ <b>Todas as fontes ativadas.</b>",
        "export_caption": "📊 Suas fontes de dados foram exportadas.",
        "processing": "⏳ Processando, aguarde...",
        "session_expired": "❌ Sessão expirada. Por favor envie o URL novamente.",
        "view_original": "🔗 Ver post original",
        "join_title": "👋 <b>Bem-vindo ao IN365Bot!</b>\n\n⚠️ Para usar este bot você deve primeiro entrar no nosso canal.\n\n1️⃣ Toque no botão para entrar em <b>@copyrightpost</b>\n2️⃣ Depois pressione <b>✅ Entrei</b>",
        "join_btn": "📢 Entrar no @copyrightpost",
        "joined_btn": "✅ Entrei",
        "not_joined": "⚠️ Você ainda não entrou!",
        "banned": "🚫 Você foi banido.",
        "icons_desc": "Descrição dos ícones:\n⚡ - extração rápida\n❌ - fonte desativada\n⚠️ - marcado como inválido\n🔄 - verificando fonte agora",
        "keywords_saved": "✅ Palavras-chave salvas.",
        "keywords_cleared": "✅ <b>Palavras-chave limpas.</b>",
        "cancelled": "❎ Cancelado.",
        "start_use_first": "Use /start primeiro.",
        "already_added": "Já adicionado ao seu feed.",
        "no_sources_export": "Sem fontes para exportar.",
        "exported": "✅ Exportado!",
        "no_history": "📋 <b>Sem histórico ainda.</b>",
        "history_header": "📋 <b>Últimos 20 posts encaminhados</b>\n\n",
        "delivery_options": "📩 <b>Opções de entrega</b>\n\nModo silencioso — posts chegam sem som de notificação.",
        "display_options": "🖥️ <b>Opções de exibição</b>",
        "silent_mode": "🤫 Modo silencioso",
        "hide_link": "🔗 Ocultar link 'Ver original'",
        "filter_header": "🗑️ <b>Filtragem por palavras-chave</b>\n\nPosts com palavra-chave no título são <b>ignorados</b>.\nAtual: <code>{kw}</code>",
        "set_keywords_prompt": "✏️ <b>Definir palavras-chave</b>\n\nEnvie palavras-chave separadas por vírgulas para ignorar.\nExemplo: <code>bitcoin, cripto, anúncios</code>",
        "moderation_header": "🕵️ <b>Modo moderação</b>\n\nQuando ATIVADO, posts inapropriados são ignorados.",
        "butler_header": "🙂 <b>Modo mordomo</b>\n\nQuando ATIVADO, o bot envia um resumo diário.",
        "free_account": "🆓 <b>Conta gratuita</b>",
        "forwarded": "📤 Mensagens encaminhadas: <b>{count} / 50</b>",
    },
    "bn": {
        "add_source_prompt": "➕ <b>উৎস যোগ করুন</b>\n\nআমাকে একটি URL পাঠান:",
        "source_added": "✅ <b>উৎস যোগ হয়েছে!</b>",
        "source_exists": "⚠️ আপনি ইতিমধ্যে এই উৎসে সাবস্ক্রাইব করেছেন।",
        "source_reactivated": "✅ উৎস পুনরায় সক্রিয় হয়েছে!",
        "source_removed": "✅ <b>মুছে ফেলা হয়েছে:</b>",
        "source_limit": "❌ উৎসের সীমা পৌঁছে গেছে ({limit})। আগে একটি সরান।",
        "url_not_recognised": "❌ URL সনাক্ত করা যায়নি।\nসমর্থিত: RSS/Atom, YouTube, Reddit, Medium, Livejournal, Instagram, Twitter, Telegram।",
        "feed_load_error": "❌ সেই URL থেকে ফিড লোড করা সম্ভব হয়নি।\n\nকিছুক্ষণ পরে আবার চেষ্টা করুন।",
        "checking_feed": "🔍 ফিড যাচাই করা হচ্ছে, অনুগ্রহ করে অপেক্ষা করুন...",
        "fetching_posts": "📥 প্রাথমিক পোস্ট আনা হচ্ছে...",
        "fetched_posts": "📤 <b>{count}</b>টি প্রাথমিক পোস্ট আনা হয়েছে",
        "sources_count": "📊 উৎস: {count}/{max}",
        "new_posts_interval": "⚡️ প্রতি ৫ মিনিটে নতুন পোস্ট!",
        "no_sources": "📋 এখনো কোনো উৎস নেই (0/{max})",
        "my_feed_header": "📂 <b>মোট {total}/{max}টি ডেটা উৎস</b>\nব্যক্তিগত + আপনার চ্যানেল + গ্রুপ।",
        "tap_to_manage": "পরিচালনার জন্য একটি উৎসে ট্যাপ করুন:",
        "settings": "⚙️ <b>সেটিংস</b>",
        "back": "◀️ পিছনে",
        "active": "✅ সক্রিয়",
        "deactivated": "❌ নিষ্ক্রিয়",
        "activate": "✅ ডেটা উৎস সক্রিয় করুন",
        "deactivate": "❌ ডেটা উৎস নিষ্ক্রিয় করুন",
        "add_btn": "➕ ডেটা উৎস যোগ করুন",
        "show_media_btn": "📸 এই উৎস থেকে সর্বশেষ মিডিয়া দেখুন",
        "remove_btn": "🗑️ ডেটা উৎস সরান",
        "confirm_add": "➕ ডেটা উৎস যোগ করুন",
        "confirm_add_media": "➕ উৎস যোগ করুন এবং মিডিয়া পাঠান",
        "no_posts_found": "📭 এই ফিডে এখন কোনো পোস্ট পাওয়া যায়নি।",
        "sent_posts": "✅ <b>{name}</b> থেকে <b>{count}</b>টি সর্বশেষ পোস্ট পাঠানো হয়েছে।",
        "all_off": "❌ <b>সমস্ত উৎস নিষ্ক্রিয়।</b>",
        "all_on": "✅ <b>সমস্ত উৎস সক্রিয়।</b>",
        "export_caption": "📊 আপনার ডেটা উৎস রপ্তানি হয়েছে।",
        "processing": "⏳ প্রক্রিয়াকরণ চলছে, অনুগ্রহ করে অপেক্ষা করুন...",
        "session_expired": "❌ সেশন শেষ। অনুগ্রহ করে আবার URL পাঠান।",
        "view_original": "🔗 মূল পোস্ট দেখুন",
        "join_title": "👋 <b>IN365Bot-এ স্বাগতম!</b>\n\n⚠️ এই বট ব্যবহার করতে আপনাকে প্রথমে আমাদের চ্যানেলে যোগ দিতে হবে।\n\n1️⃣ <b>@copyrightpost</b>-এ যোগ দিতে নিচের বোতামে ট্যাপ করুন\n2️⃣ তারপর <b>✅ আমি যোগ দিয়েছি</b> চাপুন",
        "join_btn": "📢 @copyrightpost-এ যোগ দিন",
        "joined_btn": "✅ আমি যোগ দিয়েছি",
        "not_joined": "⚠️ আপনি এখনো যোগ দেননি!",
        "banned": "🚫 আপনাকে নিষিদ্ধ করা হয়েছে।",
        "icons_desc": "আইকন বিবরণ:\n⚡ - দ্রুত ডেটা নিষ্কাশন\n❌ - উৎস নিষ্ক্রিয়\n⚠️ - অবৈধ হিসেবে চিহ্নিত\n🔄 - এখন উৎস যাচাই হচ্ছে",
        "keywords_saved": "✅ কীওয়ার্ড সংরক্ষিত হয়েছে।",
        "keywords_cleared": "✅ <b>কীওয়ার্ড মুছে ফেলা হয়েছে।</b>",
        "cancelled": "❎ বাতিল করা হয়েছে।",
        "start_use_first": "আগে /start ব্যবহার করুন।",
        "already_added": "ইতিমধ্যে আপনার ফিডে যোগ করা হয়েছে।",
        "no_sources_export": "রপ্তানির জন্য কোনো উৎস নেই।",
        "exported": "✅ রপ্তানি হয়েছে!",
        "no_history": "📋 <b>এখনো কোনো ইতিহাস নেই।</b>",
        "history_header": "📋 <b>সর্বশেষ ২০টি ফরওয়ার্ড করা পোস্ট</b>\n\n",
        "delivery_options": "📩 <b>ডেলিভারি বিকল্প</b>\n\nসাইলেন্ট মোড — পোস্ট বিজ্ঞপ্তি শব্দ ছাড়াই আসে।",
        "display_options": "🖥️ <b>প্রদর্শন বিকল্প</b>",
        "silent_mode": "🤫 সাইলেন্ট মোড",
        "hide_link": "🔗 'মূল দেখুন' লিঙ্ক লুকান",
        "filter_header": "🗑️ <b>কীওয়ার্ড ফিল্টারিং</b>\n\nশিরোনামে কীওয়ার্ড থাকা পোস্ট <b>বাদ দেওয়া হয়</b>।\nবর্তমান: <code>{kw}</code>",
        "set_keywords_prompt": "✏️ <b>কীওয়ার্ড সেট করুন</b>\n\nবাদ দেওয়ার জন্য কমা-আলাদা কীওয়ার্ড পাঠান।\nউদাহরণ: <code>বিটকয়েন, ক্রিপ্টো, বিজ্ঞাপন</code>",
        "moderation_header": "🕵️ <b>মডারেশন মোড</b>\n\nচালু থাকলে, অনুপযুক্ত কন্টেন্টের পোস্ট বাদ দেওয়া হয়।",
        "butler_header": "🙂 <b>বাটলার মোড</b>\n\nচালু থাকলে, বট প্রতিদিন ডাইজেস্ট পাঠায়।",
        "free_account": "🆓 <b>বিনামূল্যে অ্যাকাউন্ট</b>",
        "forwarded": "📤 ফরওয়ার্ড করা বার্তা: <b>{count} / 50</b>",
    },
}

def t(lang: str, key: str, **kwargs) -> str:
    """Get localized string. Falls back to English."""
    lang_dict = STRINGS.get(lang) or STRINGS["en"]
    text = lang_dict.get(key) or STRINGS["en"].get(key, key)
    if kwargs:
        try:
            text = text.format(**kwargs)
        except Exception:
            pass
    return text

def get_lang(user) -> str:
    if not user:
        return "en"
    return user.get("language") or "en"

# ════════════════════════════════════════════════════════════════════════════════
# DATABASE
# ════════════════════════════════════════════════════════════════════════════════

class DB:
    def __init__(self, url):
        self.url  = url
        self.conn = None

    def connect(self):
        self.conn = psycopg2.connect(self.url)
        self.conn.autocommit = False

    def _ensure_open(self):
        try:
            with self.conn.cursor() as c:
                c.execute("SELECT 1")
        except Exception:
            logger.warning("DB reconnecting...")
            self.connect()

    def query(self, sql, params=None):
        self._ensure_open()
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(sql, params or ())
                self.conn.commit()
                return cur.fetchall() or []
            except Exception as e:
                self.conn.rollback()
                logger.error(f"DB query: {e}")
                raise

    def one(self, sql, params=None):
        self._ensure_open()
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(sql, params or ())
                self.conn.commit()
                return cur.fetchone()
            except Exception as e:
                self.conn.rollback()
                logger.error(f"DB one: {e}")
                raise

    def run(self, sql, params=None):
        self._ensure_open()
        with self.conn.cursor() as cur:
            try:
                cur.execute(sql, params or ())
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                logger.error(f"DB run: {e}")
                raise

    def insert_id(self, sql, params=None):
        self._ensure_open()
        with self.conn.cursor() as cur:
            try:
                cur.execute(sql, params or ())
                self.conn.commit()
                row = cur.fetchone()
                return row[0] if row else None
            except Exception as e:
                self.conn.rollback()
                logger.error(f"DB insert_id: {e}")
                raise

    def init_schema(self):
        ddl = """
        CREATE TABLE IF NOT EXISTS users (
            id                  SERIAL PRIMARY KEY,
            telegram_id         BIGINT UNIQUE NOT NULL,
            username            VARCHAR(255),
            forwarded_count     INT DEFAULT 0,
            silent_mode         BOOLEAN DEFAULT FALSE,
            hide_original_link  BOOLEAN DEFAULT FALSE,
            keyword_filter      TEXT DEFAULT '',
            timezone            VARCHAR(50) DEFAULT 'UTC',
            language            VARCHAR(10) DEFAULT 'en',
            moderation_mode     BOOLEAN DEFAULT FALSE,
            butler_mode         BOOLEAN DEFAULT FALSE,
            is_banned           BOOLEAN DEFAULT FALSE,
            created_at          TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS channels (
            id          SERIAL PRIMARY KEY,
            owner_id    INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            chat_id     BIGINT UNIQUE NOT NULL,
            chat_title  VARCHAR(255),
            chat_type   VARCHAR(50),
            created_at  TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS subscriptions (
            id              SERIAL PRIMARY KEY,
            user_id         INT REFERENCES users(id) ON DELETE CASCADE,
            channel_id      INT REFERENCES channels(id) ON DELETE CASCADE,
            source_url      VARCHAR(500) NOT NULL,
            source_type     VARCHAR(50) NOT NULL,
            source_name     VARCHAR(255),
            original_url    VARCHAR(500),
            is_active       BOOLEAN DEFAULT TRUE,
            last_check      TIMESTAMP,
            initial_fetched BOOLEAN DEFAULT FALSE,
            created_at      TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS posts (
            id          SERIAL PRIMARY KEY,
            source_id   VARCHAR(64) UNIQUE NOT NULL,
            source_type VARCHAR(50),
            title       TEXT,
            url         VARCHAR(500),
            media_url   VARCHAR(500),
            created_at  TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS sent_history (
            id          SERIAL PRIMARY KEY,
            user_id     INT REFERENCES users(id) ON DELETE CASCADE,
            channel_id  INT REFERENCES channels(id) ON DELETE CASCADE,
            post_id     INT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
            sent_at     TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS user_state (
            telegram_id BIGINT PRIMARY KEY,
            state       VARCHAR(100) DEFAULT '',
            ctx         TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS broadcast_log (
            id          SERIAL PRIMARY KEY,
            sent_by     BIGINT NOT NULL,
            message     TEXT,
            sent_count  INT DEFAULT 0,
            created_at  TIMESTAMP DEFAULT NOW()
        );
        ALTER TABLE users ADD COLUMN IF NOT EXISTS forwarded_count     INT DEFAULT 0;
        ALTER TABLE users ADD COLUMN IF NOT EXISTS silent_mode         BOOLEAN DEFAULT FALSE;
        ALTER TABLE users ADD COLUMN IF NOT EXISTS hide_original_link  BOOLEAN DEFAULT FALSE;
        ALTER TABLE users ADD COLUMN IF NOT EXISTS moderation_mode     BOOLEAN DEFAULT FALSE;
        ALTER TABLE users ADD COLUMN IF NOT EXISTS butler_mode         BOOLEAN DEFAULT FALSE;
        ALTER TABLE users ADD COLUMN IF NOT EXISTS is_banned           BOOLEAN DEFAULT FALSE;
        ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS initial_fetched BOOLEAN DEFAULT FALSE;
        ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS original_url VARCHAR(500);
        ALTER TABLE posts ADD COLUMN IF NOT EXISTS media_url VARCHAR(500);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_sent_user_post    ON sent_history(user_id, post_id) WHERE user_id IS NOT NULL;
        CREATE UNIQUE INDEX IF NOT EXISTS idx_sent_chan_post    ON sent_history(channel_id, post_id) WHERE channel_id IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_subs_user    ON subscriptions(user_id);
        CREATE INDEX IF NOT EXISTS idx_subs_chan    ON subscriptions(channel_id);
        CREATE INDEX IF NOT EXISTS idx_posts_src    ON posts(source_id);
        CREATE INDEX IF NOT EXISTS idx_sent_user    ON sent_history(user_id);
        CREATE INDEX IF NOT EXISTS idx_users_tgid   ON users(telegram_id);
        CREATE INDEX IF NOT EXISTS idx_users_banned ON users(is_banned);
        """
        with self.conn.cursor() as cur:
            try:
                cur.execute(ddl)
                self.conn.commit()
                logger.info("Schema OK")
            except Exception as e:
                self.conn.rollback()
                logger.error(f"Schema error: {e}")
                raise

# ════════════════════════════════════════════════════════════════════════════════
# SOURCE DETECTION — Fixed for YouTube @handles and Instagram
# ════════════════════════════════════════════════════════════════════════════════

# Multiple public RSSHub instances as fallbacks
RSSHUB_INSTANCES = [
    "https://rsshub.app",
    "https://rsshub.rssforever.com",
    "https://hub.slarker.me",
    "https://rsshub.feeded.xyz",
]

def _try_rsshub_path(path: str) -> str | None:
    """Try each RSSHub instance and return the first working URL."""
    for base in RSSHUB_INSTANCES:
        url = f"{base}{path}"
        try:
            r = requests.get(url, headers=HTTP_HEADERS, timeout=10, allow_redirects=True)
            if r.status_code == 200:
                feed = feedparser.parse(r.content)
                if feed.entries or feed.feed.get("title"):
                    logger.info(f"RSSHub working instance: {base}")
                    return url
        except Exception:
            continue
    return None

def _resolve_youtube_channel_id(handle: str) -> str | None:
    """
    Resolve a YouTube @handle or /c/ or /user/ slug to a channel_id.
    Scrapes the YouTube page to find the canonical channel ID.
    """
    urls_to_try = [
        f"https://www.youtube.com/@{handle}",
        f"https://www.youtube.com/c/{handle}",
        f"https://www.youtube.com/user/{handle}",
    ]
    for page_url in urls_to_try:
        try:
            r = requests.get(page_url, headers=HTTP_HEADERS, timeout=15, allow_redirects=True)
            if r.status_code != 200:
                continue
            # Look for channel ID in page source
            m = re.search(r'"channelId"\s*:\s*"(UC[\w-]{22})"', r.text)
            if not m:
                m = re.search(r'channel/(UC[\w-]{22})', r.text)
            if not m:
                m = re.search(r'"externalId"\s*:\s*"(UC[\w-]{22})"', r.text)
            if m:
                logger.info(f"Resolved YouTube handle @{handle} → {m.group(1)}")
                return m.group(1)
        except Exception as e:
            logger.warning(f"YouTube resolve {page_url}: {e}")
    return None

def detect_platform(url: str):
    """
    Returns (platform_name, rss_url, original_url, needs_validation)
    All heavy network calls happen in check_rss / during the add flow.
    detect_platform itself is synchronous and fast (no network).
    For YouTube @handles, we return needs_validation=True and a special sentinel.
    """
    url = url.strip().rstrip("/")
    low = url.lower()

    # ── YouTube ────────────────────────────────────────────────────────────────
    if "youtube.com" in low or "youtu.be" in low:
        # Direct channel ID — always works
        m = re.search(r"youtube\.com/channel/(UC[\w-]+)", url)
        if m:
            rss = f"https://www.youtube.com/feeds/videos.xml?channel_id={m.group(1)}"
            return "youtube", rss, url, False

        # @handle, /c/, /user/ — need to resolve channel ID
        m = re.search(r"youtube\.com/(?:@|c/|user/)([^/?&#\s]+)", url)
        if m:
            handle = m.group(1)
            # Return sentinel; actual resolution happens in resolve_and_check
            return "youtube_handle", handle, url, True

        return None, None, None, False

    # ── Reddit ─────────────────────────────────────────────────────────────────
    if "reddit.com" in low:
        m = re.search(r"reddit\.com/r/([^/?#\s]+)", url)
        if m:
            rss = f"https://www.reddit.com/r/{m.group(1)}/.rss"
            return "reddit", rss, url, False
        m = re.search(r"reddit\.com/u(?:ser)?/([^/?#\s]+)", url)
        if m:
            rss = f"https://www.reddit.com/user/{m.group(1)}/.rss"
            return "reddit", rss, url, False
        return None, None, None, False

    # ── Medium ─────────────────────────────────────────────────────────────────
    if "medium.com" in low:
        m = re.search(r"medium\.com/(@[^/?#\s]+)", url)
        if m:
            rss = f"https://medium.com/feed/{m.group(1)}"
            return "medium", rss, url, True
        m = re.search(r"medium\.com/([^/?#\s@][^/?#\s]*)", url)
        if m:
            rss = f"https://medium.com/feed/{m.group(1)}"
            return "medium", rss, url, True
        return None, None, None, False

    # ── Livejournal ────────────────────────────────────────────────────────────
    if "livejournal.com" in low:
        m = re.search(r"([a-z0-9_-]+)\.livejournal\.com", low)
        if m:
            rss = f"https://{m.group(1)}.livejournal.com/data/rss"
            return "livejournal", rss, url, True
        return None, None, None, False

    # ── Instagram ─────────────────────────────────────────────────────────────
    if "instagram.com" in low:
        m = re.search(r"instagram\.com/([^/?#\s]+)", url)
        if m:
            username = m.group(1).strip("/")
            if username in ("p", "reel", "explore", "accounts", "stories"):
                return None, None, None, False
            return "instagram_handle", username, url, True
        return None, None, None, False

    # ── Twitter/X ─────────────────────────────────────────────────────────────
    if "twitter.com" in low or "x.com" in low:
        m = re.search(r"(?:twitter|x)\.com/([^/?#\s]+)", url)
        if m:
            username = m.group(1).strip("/")
            if username in ("i", "home", "explore", "notifications", "messages"):
                return None, None, None, False
            return "twitter_handle", username, url, True
        return None, None, None, False

    # ── Telegram channel ──────────────────────────────────────────────────────
    if "t.me" in low:
        m = re.search(r"t\.me/([^/?#\s]+)", url)
        if m:
            username = m.group(1).strip("/")
            return "telegram_handle", username, url, True
        return None, None, None, False

    # ── Generic RSS / Atom ─────────────────────────────────────────────────────
    return "rss", url, url, True


def resolve_feed(src_type: str, handle_or_url: str) -> tuple[str, str]:
    """
    For handle-based platforms, resolve the actual RSS URL.
    Returns (resolved_platform, rss_url) or (None, None) on failure.
    """
    if src_type == "youtube_handle":
        channel_id = _resolve_youtube_channel_id(handle_or_url)
        if channel_id:
            rss = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
            return "youtube", rss
        # Fallback: try user-based feed (works for some legacy channels)
        rss = f"https://www.youtube.com/feeds/videos.xml?user={handle_or_url}"
        if check_rss(rss):
            return "youtube", rss
        return None, None

    elif src_type == "instagram_handle":
        # Try multiple RSSHub instances
        path = f"/instagram/user/{handle_or_url}"
        rss = _try_rsshub_path(path)
        if rss:
            return "instagram", rss
        # Fallback: picuki via rss.app style or bibliogram
        # Try publicly known alternative paths
        alt_paths = [
            f"/picuki/user/{handle_or_url}",
            f"/instagram/media/user/id/{handle_or_url}",
        ]
        for ap in alt_paths:
            rss = _try_rsshub_path(ap)
            if rss:
                return "instagram", rss
        return None, None

    elif src_type == "twitter_handle":
        path = f"/twitter/user/{handle_or_url}"
        rss = _try_rsshub_path(path)
        if rss:
            return "twitter", rss
        # Try nitter instances as fallback
        nitter_instances = [
            "https://nitter.net",
            "https://nitter.privacydev.net",
            "https://nitter.poast.org",
        ]
        for ni in nitter_instances:
            rss_url = f"{ni}/{handle_or_url}/rss"
            if check_rss(rss_url):
                return "twitter", rss_url
        return None, None

    elif src_type == "telegram_handle":
        path = f"/telegram/channel/{handle_or_url}"
        rss = _try_rsshub_path(path)
        if rss:
            return "telegram", rss
        return None, None

    # Already resolved
    return src_type, handle_or_url


# ════════════════════════════════════════════════════════════════════════════════
# NETWORK HELPERS
# ════════════════════════════════════════════════════════════════════════════════

def check_rss(url: str) -> bool:
    try:
        r = requests.get(url, headers=HTTP_HEADERS, timeout=12, allow_redirects=True)
        if r.status_code != 200:
            return False
        feed = feedparser.parse(r.content)
        return bool(feed.entries) or bool(feed.feed.get("title"))
    except Exception:
        return False

def get_feed_title(url: str) -> str:
    try:
        r    = requests.get(url, headers=HTTP_HEADERS, timeout=12, allow_redirects=True)
        feed = feedparser.parse(r.content)
        t    = feed.feed.get("title", "").strip()
        return t[:255] if t else url[:255]
    except Exception:
        return url[:255]

def fetch_feed(rss_url: str, source_type: str, limit: int = 5) -> list:
    posts = []
    try:
        r    = requests.get(rss_url, headers=HTTP_HEADERS, timeout=20, allow_redirects=True)
        feed = feedparser.parse(r.content)
        for entry in feed.entries[:limit]:
            link = entry.get("link", "")
            if not link:
                continue
            title     = (entry.get("title") or "No title")[:500]
            uid       = entry.get("id") or link
            source_id = hashlib.md5(uid.encode("utf-8", errors="replace")).hexdigest()
            media_url = None
            if hasattr(entry, "media_thumbnail") and entry.media_thumbnail:
                media_url = entry.media_thumbnail[0].get("url")
            elif entry.get("enclosures"):
                enc = entry.enclosures[0]
                if enc.get("type", "").startswith("image"):
                    media_url = enc.get("href") or enc.get("url")
            posts.append({
                "source_id":   source_id,
                "source_type": source_type,
                "title":       title,
                "url":         link[:500],
                "media_url":   (media_url or "")[:500],
            })
    except Exception as e:
        logger.warning(f"Fetch error {rss_url}: {e}")
    return posts


# ════════════════════════════════════════════════════════════════════════════════
# KEYBOARDS (language-aware)
# ════════════════════════════════════════════════════════════════════════════════

def btn(label, data):
    return InlineKeyboardButton(label, callback_data=data)

def url_btn(label, url):
    return InlineKeyboardButton(label, url=url)

def kb_join(lang="en"):
    return InlineKeyboardMarkup([
        [url_btn(t(lang, "join_btn"), CHANNEL_INVITE_URL)],
        [btn(t(lang, "joined_btn"), "check_join")],
    ])

def kb_main(source_count, lang="en"):
    today = datetime.now(timezone.utc).strftime("%m/%d/%Y")
    back  = t(lang, "back")
    return InlineKeyboardMarkup([
        [btn("➕ Add source",                                  "add_source")],
        [btn("🚀 Direct connection",                           "direct_conn")],
        [btn("🔀 Private/Channel/Group modes",                 "modes")],
        [btn(f"📂 My feed [{source_count}]", "my_feed"), btn("⚙️ Settings", "settings")],
        [btn("📬 Contact us",                "contact"), btn("📋 History",  "history")],
        [btn("🔮 RSS generator",                               "rss_gen")],
        [btn("🏷️ Referral program",                            "referral")],
        [btn("📚 How to use this bot  ↗",                     "how_to_use")],
        [btn("📦 Local data collector",                        "data_collector")],
        [btn("💳 Premium subscription",                        "premium")],
        [btn(f"🔥 News and updates [{today}] 🔥",             "updates")],
    ])

def kb_back(dest="main", lang="en"):
    return InlineKeyboardMarkup([[btn(t(lang, "back"), dest)]])

def kb_settings(user):
    lang = get_lang(user)
    tz = (user.get("timezone") or "UTC") if user else "UTC"
    return InlineKeyboardMarkup([
        [btn("📩 Delivery options",          "s_delivery")],
        [btn("🖥️ Display options",           "s_display")],
        [btn("🗑️ Message filtering",         "s_filter")],
        [btn("🕵️ Moderation mode",           "s_moderation")],
        [btn("🙂 Butler mode",               "s_butler")],
        [btn("🤖 AI (LLM) settings",         "s_ai")],
        [btn("🐦 Your Twitter accounts",     "s_twitter")],
        [btn(f"🕐 Your timezone {tz}",       "s_timezone")],
        [btn("🌍 Change language",           "s_language")],
        [btn(t(lang, "back"),                "main")],
    ])

def kb_delivery(silent, lang="en"):
    m = "✅" if silent else "☐"
    return InlineKeyboardMarkup([
        [btn(f"{m} {t(lang, 'silent_mode')}", "tog_silent")],
        [btn(t(lang, "back"), "settings")],
    ])

def kb_display(hide, lang="en"):
    m = "✅" if hide else "☐"
    return InlineKeyboardMarkup([
        [btn(f"{m} {t(lang, 'hide_link')}", "tog_hide_link")],
        [btn(t(lang, "back"), "settings")],
    ])

def kb_filter(kw, lang="en"):
    state = "✅ Active" if kw.strip() else "☐ Inactive"
    return InlineKeyboardMarkup([
        [btn(f"{state} — keyword filter", "noop")],
        [btn("✏️ Set keywords",   "set_kw")],
        [btn("🗑️ Clear keywords", "clear_kw")],
        [btn(t(lang, "back"), "settings")],
    ])

def kb_moderation(on, lang="en"):
    m = "✅ ON" if on else "☐ OFF"
    return InlineKeyboardMarkup([
        [btn(f"{m}  🕵️ Moderation mode", "tog_moderation")],
        [btn(t(lang, "back"), "settings")],
    ])

def kb_butler(on, lang="en"):
    m = "✅ ON" if on else "☐ OFF"
    return InlineKeyboardMarkup([
        [btn(f"{m}  🙂 Butler mode", "tog_butler")],
        [btn(t(lang, "back"), "settings")],
    ])

def kb_timezone(lang="en"):
    rows = []
    for i in range(0, len(TIMEZONES), 3):
        rows.append([btn(tz, f"set_tz_{tz}") for tz in TIMEZONES[i:i+3]])
    rows.append([btn(t(lang, "back"), "settings")])
    return InlineKeyboardMarkup(rows)

def kb_language(lang="en"):
    rows = []
    items = list(LANGUAGES.items())
    for i in range(0, len(items), 2):
        rows.append([btn(label, f"set_lang_{code}") for code, label in items[i:i+2]])
    rows.append([btn(t(lang, "back"), "settings")])
    return InlineKeyboardMarkup(rows)

def kb_admin():
    return InlineKeyboardMarkup([
        [btn("📊 Stats",            "adm_stats")],
        [btn("📢 Broadcast",        "adm_broadcast"),  btn("📜 Activity log",   "adm_activity")],
        [btn("👥 List users",       "adm_users"),       btn("📡 List channels",  "adm_channels")],
        [btn("🚫 Ban user",         "adm_ban"),         btn("✅ Unban user",     "adm_unban")],
        [btn("⚡ Force poll now",   "adm_force_poll"),  btn("🗑️ Clean old posts","adm_clean")],
        [btn("📋 Banned list",      "adm_banned_list")],
    ])

def kb_my_feed(subs: list, lang="en") -> InlineKeyboardMarkup:
    rows = []
    for s in subs:
        icon   = PLATFORMS.get(s["source_type"], "📱")
        marker = "⚡" if s["is_active"] else "❌"
        name   = (s["source_name"] or s["source_url"])[:35]
        label  = f"{marker} {icon} {name}"
        rows.append([btn(label, f"feed_detail_{s['id']}")])
    rows.append([btn("💾 Export data sources to Excel", "feed_export")])
    rows.append([btn(t(lang, "all_off").replace("<b>", "").replace("</b>", ""), "feed_off_all")])
    rows.append([btn(t(lang, "all_on").replace("<b>", "").replace("</b>", ""),  "feed_on_all")])
    rows.append([btn(t(lang, "back"), "main")])
    return InlineKeyboardMarkup(rows)

def kb_feed_detail(sub_id: int, is_active: bool, lang="en") -> InlineKeyboardMarkup:
    toggle_label = t(lang, "deactivate") if is_active else t(lang, "activate")
    return InlineKeyboardMarkup([
        [btn(t(lang, "add_btn"),        f"feed_noop_{sub_id}")],
        [btn(t(lang, "show_media_btn"), f"feed_media_{sub_id}")],
        [btn(toggle_label,              f"feed_toggle_{sub_id}")],
        [btn(t(lang, "remove_btn"),     f"feed_remove_{sub_id}")],
        [btn(t(lang, "back"),           "my_feed")],
    ])

def kb_add_source_confirm(lang="en") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [btn(t(lang, "confirm_add"),       "confirm_add_source")],
        [btn(t(lang, "confirm_add_media"), "confirm_add_with_media")],
        [btn(t(lang, "back"),              "main")],
    ])


# ════════════════════════════════════════════════════════════════════════════════
# BOT
# ════════════════════════════════════════════════════════════════════════════════

class Bot:
    def __init__(self):
        self.db  = DB(DATABASE_URL)
        self.db.connect()
        self.db.init_schema()
        self.app = Application.builder().token(BOT_TOKEN).build()
        self._register()

    def _register(self):
        a = self.app
        a.add_handler(CommandHandler("start",      self.cmd_start))
        a.add_handler(CommandHandler("add",        self.cmd_add))
        a.add_handler(CommandHandler("remove",     self.cmd_remove))
        a.add_handler(CommandHandler("list",       self.cmd_list))
        a.add_handler(CommandHandler("help",       self.cmd_help))
        a.add_handler(CommandHandler("admin",      self.cmd_admin))
        a.add_handler(CommandHandler("stats",      self.cmd_admin))
        a.add_handler(CommandHandler("ban",        self.cmd_ban))
        a.add_handler(CommandHandler("unban",      self.cmd_unban))
        a.add_handler(CommandHandler("broadcast",  self.cmd_broadcast))
        a.add_handler(CommandHandler("connect",    self.cmd_connect))
        a.add_handler(CommandHandler("addchannel", self.cmd_addchannel))
        a.add_handler(CallbackQueryHandler(self.handle_cb))
        a.add_handler(ChatMemberHandler(self.on_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))
        a.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_text))
        a.post_init = self._post_init

    async def _post_init(self, app):
        asyncio.create_task(self._polling_loop(app))
        logger.info("Bot ready")

    async def _notify_owner(self, text):
        try:
            await self.app.bot.send_message(
                chat_id=OWNER_ID, text=text, parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception as e:
            logger.warning(f"Owner notify: {e}")

    async def _is_member(self, bot, tg_id):
        try:
            m = await bot.get_chat_member(REQUIRED_CHANNEL, tg_id)
            return m.status in (ChatMember.MEMBER, ChatMember.ADMINISTRATOR, ChatMember.OWNER)
        except TelegramError as e:
            logger.warning(f"Membership check {tg_id}: {e}")
            return True

    async def _gate(self, update, context):
        tg_id = update.effective_user.id
        user  = self.db.one("SELECT is_banned, language FROM users WHERE telegram_id = %s", (tg_id,))
        lang  = get_lang(user)
        if user and user.get("is_banned"):
            txt = t(lang, "banned")
            if update.message:
                await update.message.reply_text(txt)
            elif update.callback_query:
                await update.callback_query.answer(txt, show_alert=True)
            return False
        if await self._is_member(context.bot, tg_id):
            return True
        join_text = t(lang, "join_title")
        if update.message:
            await update.message.reply_text(join_text, parse_mode="HTML", reply_markup=kb_join(lang))
        elif update.callback_query:
            await update.callback_query.answer(t(lang, "not_joined"), show_alert=True)
            await update.callback_query.edit_message_text(
                join_text, parse_mode="HTML", reply_markup=kb_join(lang)
            )
        return False

    def _ensure_user(self, tg_id, username):
        uname = (username or "user")[:255]
        self.db.run(
            "INSERT INTO users (telegram_id, username) VALUES (%s, %s) "
            "ON CONFLICT (telegram_id) DO UPDATE SET username = EXCLUDED.username",
            (tg_id, uname),
        )
        return self.db.one("SELECT * FROM users WHERE telegram_id = %s", (tg_id,))

    def _source_count(self, user_id):
        row = self.db.one(
            "SELECT COUNT(*) AS c FROM subscriptions WHERE user_id=%s AND is_active=TRUE",
            (user_id,),
        )
        return row["c"] if row else 0

    def _total_sub_count(self, user_id):
        row = self.db.one(
            "SELECT COUNT(*) AS c FROM subscriptions WHERE user_id=%s", (user_id,),
        )
        return row["c"] if row else 0

    def _set_state(self, tg_id, state, ctx=""):
        self.db.run(
            "INSERT INTO user_state (telegram_id, state, ctx) VALUES (%s,%s,%s) "
            "ON CONFLICT (telegram_id) DO UPDATE SET state=EXCLUDED.state, ctx=EXCLUDED.ctx",
            (tg_id, state, ctx),
        )

    def _get_state(self, tg_id):
        row = self.db.one("SELECT state, ctx FROM user_state WHERE telegram_id=%s", (tg_id,))
        return (row["state"], row["ctx"]) if row else ("", "")

    def _clear_state(self, tg_id):
        self.db.run("DELETE FROM user_state WHERE telegram_id=%s", (tg_id,))

    def _start_text(self, user, count):
        lang      = get_lang(user)
        uname     = user.get("username") or "user"
        tg_id     = user.get("telegram_id", "")
        forwarded = user.get("forwarded_count", 0)
        return (
            f"👤 @{uname}\n"
            f"🆔 <code>{tg_id}</code>\n"
            f"{t(lang, 'free_account')}\n"
            f"{t(lang, 'forwarded', count=forwarded)}\n\n"
            f"🔥 <b>IN365Bot v2.4</b> — RSS Aggregator\n\n"
            f"<b>Supported platforms:</b>\n"
            f"📰 RSS — any RSS/Atom feed\n"
            f"▶️ YouTube — channel feeds\n"
            f"🟠 Reddit — subreddits &amp; users\n"
            f"✍️ Medium — publications\n"
            f"📝 Livejournal — user journals\n"
            f"📸 Instagram — public profiles\n"
            f"🐦 Twitter/X — public accounts\n"
            f"✈️ Telegram — public channels\n\n"
            f"<b>✨ Features:</b>\n"
            f"🔀 Private or channel/group modes\n"
            f"🖼️ Photos, videos and files delivery\n"
            f"🚀 Direct Telegram connection\n"
            f"⚡️ Fast refresh rate (5 min)\n"
            f"✂️ Filters &amp; text splitting\n"
            f"🌐 Multi-language UI (10 languages)\n"
            f"🤖 ChatGPT summarizing &amp; rephrasing\n"
            f"🕵️ Moderation &amp; butler modes\n"
            f"👥 Referral program\n\n"
            f"<b>How to Use:</b>\n"
            f"— Add a data source (RSS, YouTube, Reddit, Instagram…)\n"
            f"— Bot will forward new posts automatically!\n\n"
            f"{t(lang, 'sources_count', count=count, max=MAX_FREE_SOURCES)}"
        )

    # ════════════════════════════════════════════════════════════════════════════
    # CORE ADD LOGIC — Fixed for handle-based platforms
    # ════════════════════════════════════════════════════════════════════════════

    async def _resolve_and_add(self, send_fn, edit_fn, user, raw_url: str, send_media: bool):
        """
        Unified add logic.
        send_fn(text, **kw) — sends a new message
        edit_fn(text, **kw) — edits existing message (or sends new if None)
        """
        lang  = get_lang(user)
        tg_id = user["telegram_id"]
        count = self._source_count(user["id"])

        if count >= MAX_FREE_SOURCES:
            await edit_fn(t(lang, "source_limit", limit=MAX_FREE_SOURCES))
            return

        src_type, handle_or_url, original_url, needs_check = detect_platform(raw_url)
        if not src_type or not handle_or_url:
            await edit_fn(t(lang, "url_not_recognised"))
            return

        await edit_fn(t(lang, "checking_feed"))

        # Resolve handle → actual RSS URL
        if needs_check or src_type.endswith("_handle"):
            resolved_type, rss_url = await asyncio.to_thread(resolve_feed, src_type, handle_or_url)
            if not resolved_type or not rss_url:
                await edit_fn(t(lang, "feed_load_error"))
                return
        else:
            resolved_type = src_type
            rss_url = handle_or_url
            if needs_check:
                ok = await asyncio.to_thread(check_rss, rss_url)
                if not ok:
                    await edit_fn(t(lang, "feed_load_error"))
                    return

        feed_name = await asyncio.to_thread(get_feed_title, rss_url)

        # Insert subscription
        try:
            sub_id = self.db.insert_id(
                "INSERT INTO subscriptions "
                "(user_id, source_url, source_type, source_name, original_url, is_active, initial_fetched) "
                "VALUES (%s,%s,%s,%s,%s,TRUE,FALSE) ON CONFLICT DO NOTHING RETURNING id",
                (user["id"], rss_url, resolved_type, feed_name, original_url or rss_url),
            )
        except Exception as e:
            logger.error(f"Insert sub: {e}")
            await edit_fn("❌ Database error saving subscription.")
            return

        if sub_id is None:
            existing = self.db.one(
                "SELECT id, is_active FROM subscriptions WHERE user_id=%s AND source_url=%s",
                (user["id"], rss_url),
            )
            if existing and not existing["is_active"]:
                self.db.run("UPDATE subscriptions SET is_active=TRUE WHERE id=%s", (existing["id"],))
                await edit_fn(t(lang, "source_reactivated"))
            else:
                await edit_fn(t(lang, "source_exists"))
            return

        await edit_fn(t(lang, "fetching_posts"))

        posts      = await asyncio.to_thread(fetch_feed, rss_url, resolved_type, INITIAL_FETCH_COUNT)
        sent_count = 0
        hide       = bool(user.get("hide_original_link", False))
        silent     = bool(user.get("silent_mode", False))

        for post in posts:
            try:
                ex = self.db.one("SELECT id FROM posts WHERE source_id=%s", (post["source_id"],))
                if ex:
                    post_id = ex["id"]
                else:
                    post_id = self.db.insert_id(
                        "INSERT INTO posts (source_id, source_type, title, url, media_url) "
                        "VALUES (%s,%s,%s,%s,%s) RETURNING id",
                        (post["source_id"], post["source_type"],
                         post["title"], post["url"], post.get("media_url", "")),
                    )
                if post_id is None:
                    continue

                icon     = PLATFORMS.get(post["source_type"], "📱")
                msg_text = f"{icon} <b>{feed_name}</b>\n\n{post['title']}"
                if not hide:
                    msg_text += f"\n\n<a href='{post['url']}'>{t(lang, 'view_original')}</a>"

                if send_media and post.get("media_url"):
                    try:
                        await self.app.bot.send_photo(
                            chat_id=tg_id, photo=post["media_url"],
                            caption=msg_text[:1024], parse_mode="HTML",
                            disable_notification=silent,
                        )
                    except TelegramError:
                        await self.app.bot.send_message(
                            chat_id=tg_id, text=msg_text, parse_mode="HTML",
                            disable_notification=silent,
                        )
                else:
                    await self.app.bot.send_message(
                        chat_id=tg_id, text=msg_text, parse_mode="HTML",
                        disable_notification=silent,
                    )

                self.db.run(
                    "INSERT INTO sent_history (user_id, post_id) VALUES (%s,%s) "
                    "ON CONFLICT DO NOTHING",
                    (user["id"], post_id),
                )
                self.db.run(
                    "UPDATE users SET forwarded_count=forwarded_count+1 WHERE id=%s", (user["id"],)
                )
                sent_count += 1
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"Initial post send: {e}")

        self.db.run("UPDATE subscriptions SET initial_fetched=TRUE WHERE id=%s", (sub_id,))
        count = self._source_count(user["id"])
        icon  = PLATFORMS.get(resolved_type, "📱")
        result = (
            f"{t(lang, 'source_added')}\n\n"
            f"{icon} <b>{resolved_type.upper()}</b>\n"
            f"📌 {feed_name}\n\n"
            f"{t(lang, 'fetched_posts', count=sent_count)}\n"
            f"{t(lang, 'sources_count', count=count, max=MAX_FREE_SOURCES)}\n\n"
            f"{t(lang, 'new_posts_interval')}"
        )
        await edit_fn(result, parse_mode="HTML")

    # ════════════════════════════════════════════════════════════════════════════
    # COMMANDS
    # ════════════════════════════════════════════════════════════════════════════

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._gate(update, context):
            return
        tg_id    = update.effective_user.id
        username = update.effective_user.username or "user"
        existing = self.db.one("SELECT id FROM users WHERE telegram_id=%s", (tg_id,))
        is_new   = existing is None
        user     = self._ensure_user(tg_id, username)
        lang     = get_lang(user)
        count    = self._source_count(user["id"])
        await update.message.reply_text(
            self._start_text(user, count), parse_mode="HTML", reply_markup=kb_main(count, lang),
        )
        if is_new:
            ulink = f"@{username}" if username != "user" else f"ID {tg_id}"
            await self._notify_owner(
                f"🆕 <b>New user joined!</b>\n\n"
                f"👤 {ulink}\n🆔 <code>{tg_id}</code>\n"
                f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC"
            )

    async def cmd_add(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._gate(update, context):
            return
        tg_id    = update.effective_user.id
        username = update.effective_user.username or "user"
        user     = self._ensure_user(tg_id, username)
        lang     = get_lang(user)

        if not context.args:
            await update.message.reply_text(
                f"➕ <b>Add a source</b>\n\n<b>Usage examples:</b>\n"
                f"<code>/add https://feeds.bbci.co.uk/news/rss.xml</code>\n"
                f"<code>/add https://youtube.com/@channelname</code>\n"
                f"<code>/add https://reddit.com/r/python</code>\n"
                f"<code>/add https://medium.com/@username</code>\n"
                f"<code>/add https://instagram.com/username</code>\n"
                f"<code>/add https://twitter.com/username</code>",
                parse_mode="HTML",
            )
            return

        raw_url = context.args[0]
        status_msg = await update.message.reply_text(t(lang, "checking_feed"))

        async def edit_fn(text, **kw):
            try:
                await status_msg.edit_text(text, **kw)
            except Exception:
                await update.message.reply_text(text, **kw)

        async def send_fn(text, **kw):
            await update.message.reply_text(text, **kw)

        await self._resolve_and_add(send_fn, edit_fn, user, raw_url, send_media=False)

    async def cmd_remove(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._gate(update, context):
            return
        tg_id = update.effective_user.id
        user  = self.db.one("SELECT * FROM users WHERE telegram_id=%s", (tg_id,))
        lang  = get_lang(user)
        if not user:
            await update.message.reply_text(t(lang, "start_use_first"))
            return
        subs = self.db.query(
            "SELECT id, source_type, source_name, source_url, is_active FROM subscriptions "
            "WHERE user_id=%s ORDER BY created_at", (user["id"],),
        )
        if not subs:
            await update.message.reply_text(t(lang, "no_sources", max=MAX_FREE_SOURCES))
            return
        if context.args and context.args[0].isdigit():
            idx = int(context.args[0]) - 1
            if 0 <= idx < len(subs):
                self.db.run("UPDATE subscriptions SET is_active=FALSE WHERE id=%s", (subs[idx]["id"],))
                await update.message.reply_text(
                    f"{t(lang, 'source_removed')} {subs[idx]['source_name'] or subs[idx]['source_url']}"
                )
            else:
                await update.message.reply_text(f"❌ Range is 1–{len(subs)}")
            return
        total = self._total_sub_count(user["id"])
        text  = (
            f"{t(lang, 'my_feed_header', total=total, max=MAX_FREE_SOURCES)}\n\n"
            f"{t(lang, 'tap_to_manage')}"
        )
        await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb_my_feed(subs, lang))

    async def cmd_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._gate(update, context):
            return
        tg_id = update.effective_user.id
        user  = self.db.one("SELECT * FROM users WHERE telegram_id=%s", (tg_id,))
        lang  = get_lang(user)
        if not user:
            await update.message.reply_text(t(lang, "start_use_first"))
            return
        subs = self.db.query(
            "SELECT id, source_type, source_name, source_url, is_active "
            "FROM subscriptions WHERE user_id=%s ORDER BY created_at", (user["id"],),
        )
        if not subs:
            await update.message.reply_text(t(lang, "no_sources", max=MAX_FREE_SOURCES))
            return
        total = len(subs)
        text  = (
            f"{t(lang, 'my_feed_header', total=total, max=MAX_FREE_SOURCES)}\n\n"
            f"{t(lang, 'tap_to_manage')}"
        )
        await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb_my_feed(subs, lang))

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._gate(update, context):
            return
        await update.message.reply_text(
            "❓ <b>IN365Bot — Help</b>\n\n"
            "<b>Add sources:</b>\n"
            "<code>/add https://feeds.bbci.co.uk/news/rss.xml</code>\n"
            "<code>/add https://youtube.com/@channelname</code>\n"
            "<code>/add https://reddit.com/r/python</code>\n"
            "<code>/add https://instagram.com/username</code>\n"
            "<code>/add https://twitter.com/username</code>\n\n"
            "<b>Manage:</b>\n"
            "<code>/list</code>   — view all sources\n"
            "<code>/remove</code> — manage sources\n\n"
            f"Refresh: every 5 min | Limit: {MAX_FREE_SOURCES} sources\n\n"
            f"📢 Support: <a href='{CHANNEL_INVITE_URL}'>@copyrightpost</a>",
            parse_mode="HTML", disable_web_page_preview=True,
        )

    async def cmd_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != OWNER_ID:
            return
        await update.message.reply_text(
            "🛠️ <b>Admin Panel</b>", parse_mode="HTML", reply_markup=kb_admin(),
        )

    async def cmd_ban(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != OWNER_ID:
            return
        if not context.args or not context.args[0].isdigit():
            await update.message.reply_text("Usage: <code>/ban &lt;telegram_id&gt;</code>", parse_mode="HTML")
            return
        target_id = int(context.args[0])
        u = self.db.one("SELECT id, username FROM users WHERE telegram_id=%s", (target_id,))
        if not u:
            await update.message.reply_text("❌ User not found.")
            return
        self.db.run("UPDATE users SET is_banned=TRUE WHERE telegram_id=%s", (target_id,))
        await update.message.reply_text(
            f"🚫 Banned @{u['username'] or '?'} (<code>{target_id}</code>)", parse_mode="HTML"
        )

    async def cmd_unban(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != OWNER_ID:
            return
        if not context.args or not context.args[0].isdigit():
            await update.message.reply_text("Usage: <code>/unban &lt;telegram_id&gt;</code>", parse_mode="HTML")
            return
        target_id = int(context.args[0])
        self.db.run("UPDATE users SET is_banned=FALSE WHERE telegram_id=%s", (target_id,))
        await update.message.reply_text(f"✅ Unbanned <code>{target_id}</code>", parse_mode="HTML")

    async def cmd_broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != OWNER_ID:
            return
        if not context.args:
            await update.message.reply_text(
                "📢 Usage: <code>/broadcast Your message</code>", parse_mode="HTML",
            )
            return
        await self._do_broadcast(update, " ".join(context.args))

    async def _do_broadcast(self, update, message_text):
        users      = self.db.query("SELECT telegram_id FROM users WHERE is_banned=FALSE")
        sent       = 0
        failed     = 0
        status_msg = await update.message.reply_text(f"📢 Broadcasting to {len(users)} users...")
        for row in users:
            try:
                await self.app.bot.send_message(
                    chat_id=row["telegram_id"],
                    text=f"📢 <b>Announcement</b>\n\n{message_text}",
                    parse_mode="HTML",
                )
                sent += 1
                await asyncio.sleep(0.05)
            except TelegramError as e:
                failed += 1
                logger.warning(f"Broadcast fail {row['telegram_id']}: {e}")
        self.db.run(
            "INSERT INTO broadcast_log (sent_by, message, sent_count) VALUES (%s,%s,%s)",
            (OWNER_ID, message_text[:1000], sent),
        )
        await status_msg.edit_text(
            f"✅ <b>Broadcast complete</b>\n\n✅ Sent: {sent}\n❌ Failed: {failed}",
            parse_mode="HTML",
        )

    async def cmd_connect(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._gate(update, context):
            return
        tg_id = update.effective_user.id
        user  = self.db.one("SELECT * FROM users WHERE telegram_id=%s", (tg_id,))
        if not user:
            await update.message.reply_text("Use /start first.")
            return
        channels = self.db.query(
            "SELECT chat_id, chat_title FROM channels WHERE owner_id=%s", (user["id"],)
        )
        if not channels:
            await update.message.reply_text(
                "📡 No channels connected yet.\n\n"
                "Add the bot as <b>admin</b> to your channel/group — "
                "it registers automatically.", parse_mode="HTML",
            )
            return
        text = "📡 <b>Your connected channels/groups:</b>\n\n"
        for ch in channels:
            text += f"• <b>{ch['chat_title']}</b>\n  ID: <code>{ch['chat_id']}</code>\n\n"
        await update.message.reply_text(text, parse_mode="HTML")

    async def cmd_addchannel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._gate(update, context):
            return
        await update.message.reply_text(
            "📡 <b>Connect a channel/group</b>\n\n"
            "1. Add the bot as <b>administrator</b> to your channel/group.\n"
            "2. Bot registers automatically.\n3. Use /connect to confirm.",
            parse_mode="HTML",
        )

    # ════════════════════════════════════════════════════════════════════════════
    # CALLBACK HANDLER
    # ════════════════════════════════════════════════════════════════════════════

    async def handle_cb(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data  = query.data
        tg_id = update.effective_user.id

        if data == "check_join":
            if await self._is_member(context.bot, tg_id):
                user  = self._ensure_user(tg_id, update.effective_user.username or "user")
                lang  = get_lang(user)
                count = self._source_count(user["id"])
                await query.edit_message_text(
                    self._start_text(user, count), parse_mode="HTML", reply_markup=kb_main(count, lang),
                )
            else:
                user = self.db.one("SELECT language FROM users WHERE telegram_id=%s", (tg_id,))
                lang = get_lang(user)
                await query.answer(t(lang, "not_joined"), show_alert=True)
            return

        if data.startswith("adm_"):
            if tg_id != OWNER_ID:
                await query.answer("⛔ Admin only!", show_alert=True)
                return
            await self._handle_admin_cb(query, data)
            return

        if data.startswith("set_tz_") or data.startswith("set_lang_"):
            user = self.db.one("SELECT * FROM users WHERE telegram_id=%s", (tg_id,))
            lang = get_lang(user)
            if not user:
                await query.answer(t(lang, "start_use_first"), show_alert=True)
                return
            if data.startswith("set_tz_"):
                tz = data[len("set_tz_"):]
                if tz in TIMEZONES:
                    self.db.run("UPDATE users SET timezone=%s WHERE telegram_id=%s", (tz, tg_id))
                    await query.edit_message_text(
                        f"✅ Timezone set to <b>{tz}</b>", parse_mode="HTML",
                        reply_markup=kb_back("settings", lang),
                    )
            else:
                new_lang = data[len("set_lang_"):]
                if new_lang in LANGUAGES:
                    self.db.run("UPDATE users SET language=%s WHERE telegram_id=%s", (new_lang, tg_id))
                    # Refresh user to get new lang
                    user = self.db.one("SELECT * FROM users WHERE telegram_id=%s", (tg_id,))
                    await query.edit_message_text(
                        f"✅ Language set to <b>{LANGUAGES[new_lang]}</b>", parse_mode="HTML",
                        reply_markup=kb_back("settings", new_lang),
                    )
            return

        if data.startswith("feed_"):
            if not await self._gate(update, context):
                return
            user = self.db.one("SELECT * FROM users WHERE telegram_id=%s", (tg_id,))
            if not user:
                await query.answer("Use /start first", show_alert=True)
                return
            await self._handle_feed_cb(query, data, user, tg_id)
            return

        # Confirm add after URL paste
        if data in ("confirm_add_source", "confirm_add_with_media"):
            if not await self._gate(update, context):
                return
            user = self.db.one("SELECT * FROM users WHERE telegram_id=%s", (tg_id,))
            lang = get_lang(user)
            if not user:
                await query.answer(t(lang, "start_use_first"), show_alert=True)
                return
            state, ctx_url = self._get_state(tg_id)
            # BUG FIX: state was "confirm_url" but we were checking "add_source"
            if state != "confirm_url" or not ctx_url:
                self._clear_state(tg_id)
                await query.edit_message_text(t(lang, "session_expired"))
                return
            self._clear_state(tg_id)
            send_media = (data == "confirm_add_with_media")
            await query.edit_message_text(t(lang, "processing"))

            async def edit_fn(text, **kw):
                try:
                    await query.edit_message_text(text, **kw)
                except Exception:
                    pass

            await self._resolve_and_add(edit_fn, edit_fn, user, ctx_url, send_media)
            return

        if not await self._gate(update, context):
            return

        user = self.db.one("SELECT * FROM users WHERE telegram_id=%s", (tg_id,))
        lang = get_lang(user)

        if data == "main":
            if not user:
                user = self._ensure_user(tg_id, update.effective_user.username or "user")
                lang = get_lang(user)
            count = self._source_count(user["id"])
            await query.edit_message_text(
                self._start_text(user, count), parse_mode="HTML", reply_markup=kb_main(count, lang),
            )

        elif data == "add_source":
            self._set_state(tg_id, "add_source", "")
            await query.edit_message_text(
                t(lang, "add_source_prompt") + "\n\n"
                "<code>https://feeds.bbci.co.uk/news/rss.xml</code>\n"
                "<code>https://youtube.com/@channelname</code>\n"
                "<code>https://reddit.com/r/python</code>\n"
                "<code>https://medium.com/@username</code>\n"
                "<code>https://instagram.com/username</code>\n"
                "<code>https://twitter.com/username</code>",
                parse_mode="HTML", reply_markup=kb_back("main", lang),
            )

        elif data == "my_feed":
            subs = self.db.query(
                "SELECT id, source_type, source_name, source_url, is_active "
                "FROM subscriptions WHERE user_id=%s ORDER BY created_at",
                (user["id"],),
            ) if user else []
            total = len(subs)
            text  = t(lang, "my_feed_header", total=total, max=MAX_FREE_SOURCES)
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb_my_feed(subs, lang))

        elif data == "settings":
            await query.edit_message_text(
                t(lang, "settings"), parse_mode="HTML", reply_markup=kb_settings(user),
            )

        elif data == "s_delivery":
            silent = bool(user.get("silent_mode", False)) if user else False
            await query.edit_message_text(
                t(lang, "delivery_options"),
                parse_mode="HTML", reply_markup=kb_delivery(silent, lang),
            )

        elif data == "tog_silent":
            if user:
                new = not bool(user.get("silent_mode", False))
                self.db.run("UPDATE users SET silent_mode=%s WHERE telegram_id=%s", (new, tg_id))
                user = self.db.one("SELECT * FROM users WHERE telegram_id=%s", (tg_id,))
                lang = get_lang(user)
            await query.edit_message_text(
                t(lang, "delivery_options"), parse_mode="HTML",
                reply_markup=kb_delivery(bool(user.get("silent_mode", False)) if user else False, lang),
            )

        elif data == "s_display":
            hide = bool(user.get("hide_original_link", False)) if user else False
            await query.edit_message_text(
                t(lang, "display_options"), parse_mode="HTML", reply_markup=kb_display(hide, lang),
            )

        elif data == "tog_hide_link":
            if user:
                new = not bool(user.get("hide_original_link", False))
                self.db.run("UPDATE users SET hide_original_link=%s WHERE telegram_id=%s", (new, tg_id))
                user = self.db.one("SELECT * FROM users WHERE telegram_id=%s", (tg_id,))
                lang = get_lang(user)
            hide = bool(user.get("hide_original_link", False)) if user else False
            await query.edit_message_text(
                t(lang, "display_options"), parse_mode="HTML", reply_markup=kb_display(hide, lang),
            )

        elif data == "s_filter":
            kw = (user.get("keyword_filter", "") or "") if user else ""
            await query.edit_message_text(
                t(lang, "filter_header", kw=kw or "none"),
                parse_mode="HTML", reply_markup=kb_filter(kw, lang),
            )

        elif data == "set_kw":
            self._set_state(tg_id, "set_kw", "")
            await query.edit_message_text(
                t(lang, "set_keywords_prompt"),
                parse_mode="HTML", reply_markup=kb_back("settings", lang),
            )

        elif data == "clear_kw":
            if user:
                self.db.run("UPDATE users SET keyword_filter='' WHERE telegram_id=%s", (tg_id,))
            await query.edit_message_text(
                t(lang, "keywords_cleared"), parse_mode="HTML", reply_markup=kb_filter("", lang),
            )

        elif data == "s_moderation":
            on = bool(user.get("moderation_mode", False)) if user else False
            await query.edit_message_text(
                t(lang, "moderation_header"),
                parse_mode="HTML", reply_markup=kb_moderation(on, lang),
            )

        elif data == "tog_moderation":
            if user:
                new = not bool(user.get("moderation_mode", False))
                self.db.run("UPDATE users SET moderation_mode=%s WHERE telegram_id=%s", (new, tg_id))
                user = self.db.one("SELECT * FROM users WHERE telegram_id=%s", (tg_id,))
                lang = get_lang(user)
            on = bool(user.get("moderation_mode", False)) if user else False
            await query.edit_message_text(
                t(lang, "moderation_header"), parse_mode="HTML", reply_markup=kb_moderation(on, lang),
            )

        elif data == "s_butler":
            on = bool(user.get("butler_mode", False)) if user else False
            await query.edit_message_text(
                t(lang, "butler_header"), parse_mode="HTML", reply_markup=kb_butler(on, lang),
            )

        elif data == "tog_butler":
            if user:
                new = not bool(user.get("butler_mode", False))
                self.db.run("UPDATE users SET butler_mode=%s WHERE telegram_id=%s", (new, tg_id))
                user = self.db.one("SELECT * FROM users WHERE telegram_id=%s", (tg_id,))
                lang = get_lang(user)
            on = bool(user.get("butler_mode", False)) if user else False
            await query.edit_message_text(
                t(lang, "butler_header"), parse_mode="HTML", reply_markup=kb_butler(on, lang),
            )

        elif data == "s_ai":
            await query.edit_message_text(
                "🤖 <b>AI (LLM) settings</b>\n\n"
                "ChatGPT-powered features:\n• Auto-summarize long posts\n"
                "• Rephrase titles\n• Translate to your language\n\n"
                f"⭐ <b>Premium feature</b> — contact: <a href='{CHANNEL_INVITE_URL}'>@copyrightpost</a>",
                parse_mode="HTML", disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup([
                    [url_btn("💬 Contact for Premium", CHANNEL_INVITE_URL)],
                    [btn(t(lang, "back"), "settings")],
                ]),
            )

        elif data == "s_twitter":
            await query.edit_message_text(
                "🐦 <b>Your Twitter accounts</b>\n\n"
                "Use <code>/add https://twitter.com/username</code> or "
                "<code>/add https://x.com/username</code>\n\n"
                f"⭐ Enhanced features — contact: <a href='{CHANNEL_INVITE_URL}'>@copyrightpost</a>",
                parse_mode="HTML", disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup([
                    [url_btn("💬 Contact for Premium", CHANNEL_INVITE_URL)],
                    [btn(t(lang, "back"), "settings")],
                ]),
            )

        elif data == "s_timezone":
            tz = (user.get("timezone") or "UTC") if user else "UTC"
            await query.edit_message_text(
                f"🕐 <b>Your timezone</b>\n\nCurrent: <b>{tz}</b>\n\nSelect new timezone:",
                parse_mode="HTML", reply_markup=kb_timezone(lang),
            )

        elif data == "s_language":
            cur_lang  = (user.get("language") or "en") if user else "en"
            label = LANGUAGES.get(cur_lang, cur_lang)
            await query.edit_message_text(
                f"🌍 <b>Change language</b>\n\nCurrent: <b>{label}</b>\n\nSelect language:",
                parse_mode="HTML", reply_markup=kb_language(lang),
            )

        elif data == "history":
            rows = self.db.query(
                "SELECT p.title, p.url, p.source_type FROM sent_history sh "
                "JOIN posts p ON sh.post_id=p.id WHERE sh.user_id=%s "
                "ORDER BY sh.sent_at DESC LIMIT 20",
                (user["id"],),
            ) if user else []
            if not rows:
                await query.edit_message_text(
                    t(lang, "no_history"), parse_mode="HTML", reply_markup=kb_back("main", lang)
                )
                return
            text = t(lang, "history_header")
            for r in rows:
                icon  = PLATFORMS.get(r["source_type"], "📱")
                title = (r["title"] or "No title")[:55]
                text += f"{icon} <a href='{r['url']}'>{title}</a>\n"
            await query.edit_message_text(
                text, parse_mode="HTML", disable_web_page_preview=True,
                reply_markup=kb_back("main", lang),
            )

        elif data == "contact":
            await query.edit_message_text(
                "📬 <b>Contact &amp; Support</b>\n\n"
                f"📢 <a href='{CHANNEL_INVITE_URL}'>@copyrightpost</a>\n\nWe read every message!",
                parse_mode="HTML", disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup([
                    [url_btn("💬 Open channel", CHANNEL_INVITE_URL)],
                    [btn(t(lang, "back"), "main")],
                ]),
            )

        elif data == "rss_gen":
            await query.edit_message_text(
                "🔮 <b>RSS Generator Tools</b>\n\n"
                "• <a href='https://rsshub.app'>RSSHub</a> — universal hub\n"
                "• <a href='https://rss-bridge.org'>RSS-Bridge</a> — open source\n"
                "• <a href='https://fetchrss.com'>FetchRSS</a> — drag &amp; drop\n"
                "• <a href='https://politepol.com'>PolitePol</a> — any webpage\n\n"
                "Generate a feed URL, then use /add",
                parse_mode="HTML", disable_web_page_preview=True, reply_markup=kb_back("main", lang),
            )

        elif data == "referral":
            bot_link = f"https://t.me/{BOT_USERNAME}?start=ref_{tg_id}"
            await query.edit_message_text(
                f"🏷️ <b>Referral Program</b>\n\nYour link:\n<code>{bot_link}</code>\n\n"
                f"Invite friends — earn rewards!\n\n"
                f"📢 <a href='{CHANNEL_INVITE_URL}'>@copyrightpost</a>",
                parse_mode="HTML", disable_web_page_preview=True, reply_markup=kb_back("main", lang),
            )

        elif data == "how_to_use":
            await query.edit_message_text(
                "📚 <b>How to use IN365Bot</b>\n\n"
                "<b>Step 1</b> — Tap ➕ Add source or use /add URL\n\n"
                "<b>Step 2</b> — Bot checks every 5 min and delivers new posts\n\n"
                "<b>Step 3</b> — /list to view sources | tap a source to manage\n\n"
                f"❓ Support: <a href='{CHANNEL_INVITE_URL}'>@copyrightpost</a>",
                parse_mode="HTML", disable_web_page_preview=True, reply_markup=kb_back("main", lang),
            )

        elif data == "direct_conn":
            await query.edit_message_text(
                "🚀 <b>Direct Connection</b>\n\nForward to:\n"
                "• This private chat (default)\n• A Telegram channel\n• A group chat\n\n"
                "<b>Connect channel/group:</b>\n1. Add bot as admin\n2. Use /connect",
                parse_mode="HTML", reply_markup=kb_back("main", lang),
            )

        elif data == "modes":
            await query.edit_message_text(
                "🔀 <b>Private / Channel / Group modes</b>\n\n"
                "🔒 <b>Private</b> — posts arrive in this DM.\n\n"
                "📢 <b>Channel</b> — add bot as admin, posts go to the channel.\n\n"
                "👥 <b>Group</b> — add bot as admin, posts go to the group.\n\n"
                "Use /connect to see connected channels/groups.",
                parse_mode="HTML", reply_markup=kb_back("main", lang),
            )

        elif data == "data_collector":
            total_subs = self._source_count(user["id"]) if user else 0
            row        = self.db.one(
                "SELECT COUNT(*) AS c FROM sent_history WHERE user_id=%s", (user["id"],)
            ) if user else None
            total_sent = row["c"] if row else 0
            await query.edit_message_text(
                f"📦 <b>Your data</b>\n\n"
                f"📡 Active sources:   <b>{total_subs}</b>\n"
                f"📤 Posts forwarded:  <b>{total_sent}</b>\n"
                f"🤫 Silent mode:      {'✅ On' if user.get('silent_mode') else '☐ Off'}\n"
                f"🔗 Hide links:       {'✅ On' if user.get('hide_original_link') else '☐ Off'}\n"
                f"🕵️ Moderation mode:  {'✅ On' if user.get('moderation_mode') else '☐ Off'}\n"
                f"🙂 Butler mode:      {'✅ On' if user.get('butler_mode') else '☐ Off'}\n"
                f"🔍 Keyword filter:   <code>{user.get('keyword_filter') or 'none'}</code>\n"
                f"🕐 Timezone:         {user.get('timezone') or 'UTC'}\n"
                f"🌍 Language:         {LANGUAGES.get(user.get('language','en'), 'English')}",
                parse_mode="HTML", reply_markup=kb_back("main", lang),
            )

        elif data == "premium":
            await query.edit_message_text(
                f"💳 <b>Premium subscription</b>\n\n"
                f"<b>Free plan:</b>\n• {MAX_FREE_SOURCES} sources\n• 5-min refresh\n• Basic filters\n\n"
                f"<b>Premium plan:</b>\n• Unlimited sources\n• 1-min refresh\n"
                f"• AI summarization &amp; translation\n• Twitter/X integration\n\n"
                f"📢 <a href='{CHANNEL_INVITE_URL}'>@copyrightpost</a>",
                parse_mode="HTML", disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup([
                    [url_btn("💬 Get Premium", CHANNEL_INVITE_URL)],
                    [btn(t(lang, "back"), "main")],
                ]),
            )

        elif data == "updates":
            await query.edit_message_text(
                f"🔥 <b>News &amp; Updates</b>\n\n"
                f"📢 <a href='{CHANNEL_INVITE_URL}'>@copyrightpost</a>",
                parse_mode="HTML", disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup([
                    [url_btn("🔔 Open channel", CHANNEL_INVITE_URL)],
                    [btn(t(lang, "back"), "main")],
                ]),
            )

        elif data == "noop":
            pass

    # ════════════════════════════════════════════════════════════════════════════
    # FEED DETAIL CALLBACKS
    # ════════════════════════════════════════════════════════════════════════════

    async def _handle_feed_cb(self, query, data: str, user, tg_id: int):
        lang = get_lang(user)

        if data.startswith("feed_detail_"):
            sub_id = int(data.split("_")[-1])
            sub = self.db.one(
                "SELECT * FROM subscriptions WHERE id=%s AND user_id=%s",
                (sub_id, user["id"]),
            )
            if not sub:
                await query.answer("Source not found.", show_alert=True)
                return
            icon       = PLATFORMS.get(sub["source_type"], "📱")
            name       = sub["source_name"] or sub["source_url"]
            orig_url   = sub.get("original_url") or sub["source_url"]
            is_active  = bool(sub["is_active"])
            status_txt = t(lang, "active") if is_active else t(lang, "deactivated")
            text = (
                f"<b>{name}</b>\n"
                f"<a href='{orig_url}'>{orig_url[:60]}</a>\n\n"
                f"{t(lang, 'icons_desc')}\n\n"
                f"Status: {status_txt}\n"
                f"Type: {icon} {sub['source_type'].upper()}"
            )
            await query.edit_message_text(
                text, parse_mode="HTML", disable_web_page_preview=True,
                reply_markup=kb_feed_detail(sub_id, is_active, lang),
            )

        elif data.startswith("feed_toggle_"):
            sub_id = int(data.split("_")[-1])
            sub = self.db.one(
                "SELECT * FROM subscriptions WHERE id=%s AND user_id=%s",
                (sub_id, user["id"]),
            )
            if not sub:
                await query.answer("Source not found.", show_alert=True)
                return
            new_active = not bool(sub["is_active"])
            self.db.run("UPDATE subscriptions SET is_active=%s WHERE id=%s", (new_active, sub_id))
            await query.answer(
                ("activated ✅" if new_active else "deactivated ❌"), show_alert=False
            )
            sub = self.db.one("SELECT * FROM subscriptions WHERE id=%s", (sub_id,))
            icon       = PLATFORMS.get(sub["source_type"], "📱")
            name       = sub["source_name"] or sub["source_url"]
            orig_url   = sub.get("original_url") or sub["source_url"]
            is_active  = bool(sub["is_active"])
            status_txt = t(lang, "active") if is_active else t(lang, "deactivated")
            text = (
                f"<b>{name}</b>\n"
                f"<a href='{orig_url}'>{orig_url[:60]}</a>\n\n"
                f"{t(lang, 'icons_desc')}\n\n"
                f"Status: {status_txt}\n"
                f"Type: {icon} {sub['source_type'].upper()}"
            )
            await query.edit_message_text(
                text, parse_mode="HTML", disable_web_page_preview=True,
                reply_markup=kb_feed_detail(sub_id, is_active, lang),
            )

        elif data.startswith("feed_remove_"):
            sub_id = int(data.split("_")[-1])
            sub = self.db.one(
                "SELECT * FROM subscriptions WHERE id=%s AND user_id=%s",
                (sub_id, user["id"]),
            )
            if not sub:
                await query.answer("Source not found.", show_alert=True)
                return
            name = sub["source_name"] or sub["source_url"]
            self.db.run("DELETE FROM subscriptions WHERE id=%s", (sub_id,))
            subs = self.db.query(
                "SELECT id, source_type, source_name, source_url, is_active "
                "FROM subscriptions WHERE user_id=%s ORDER BY created_at",
                (user["id"],),
            )
            total = len(subs)
            text  = (
                f"{t(lang, 'source_removed')} {name}\n\n"
                f"{t(lang, 'my_feed_header', total=total, max=MAX_FREE_SOURCES)}"
            )
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb_my_feed(subs, lang))

        elif data.startswith("feed_media_"):
            sub_id = int(data.split("_")[-1])
            sub = self.db.one(
                "SELECT * FROM subscriptions WHERE id=%s AND user_id=%s",
                (sub_id, user["id"]),
            )
            if not sub:
                await query.answer("Source not found.", show_alert=True)
                return
            await query.answer("Fetching latest posts...", show_alert=False)
            posts = await asyncio.to_thread(fetch_feed, sub["source_url"], sub["source_type"], 5)
            if not posts:
                await query.edit_message_text(
                    t(lang, "no_posts_found"),
                    reply_markup=kb_feed_detail(sub_id, bool(sub["is_active"]), lang),
                )
                return
            name = sub["source_name"] or sub["source_url"]
            icon = PLATFORMS.get(sub["source_type"], "📱")
            sent = 0
            for post in posts:
                msg_text = (
                    f"{icon} <b>{name}</b>\n\n{post['title']}\n\n"
                    f"<a href='{post['url']}'>{t(lang, 'view_original')}</a>"
                )
                try:
                    if post.get("media_url"):
                        try:
                            await self.app.bot.send_photo(
                                chat_id=tg_id, photo=post["media_url"],
                                caption=msg_text[:1024], parse_mode="HTML",
                            )
                        except TelegramError:
                            await self.app.bot.send_message(
                                chat_id=tg_id, text=msg_text, parse_mode="HTML",
                            )
                    else:
                        await self.app.bot.send_message(
                            chat_id=tg_id, text=msg_text, parse_mode="HTML",
                        )
                    sent += 1
                    await asyncio.sleep(0.4)
                except TelegramError as e:
                    logger.warning(f"feed_media send: {e}")
            await query.edit_message_text(
                t(lang, "sent_posts", count=sent, name=name), parse_mode="HTML",
                reply_markup=kb_feed_detail(sub_id, bool(sub["is_active"]), lang),
            )

        elif data == "feed_off_all":
            self.db.run("UPDATE subscriptions SET is_active=FALSE WHERE user_id=%s", (user["id"],))
            subs = self.db.query(
                "SELECT id, source_type, source_name, source_url, is_active "
                "FROM subscriptions WHERE user_id=%s ORDER BY created_at", (user["id"],),
            )
            total = len(subs)
            text  = (
                f"{t(lang, 'all_off')}\n\n"
                f"{t(lang, 'my_feed_header', total=total, max=MAX_FREE_SOURCES)}"
            )
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb_my_feed(subs, lang))

        elif data == "feed_on_all":
            self.db.run("UPDATE subscriptions SET is_active=TRUE WHERE user_id=%s", (user["id"],))
            subs = self.db.query(
                "SELECT id, source_type, source_name, source_url, is_active "
                "FROM subscriptions WHERE user_id=%s ORDER BY created_at", (user["id"],),
            )
            total = len(subs)
            text  = (
                f"{t(lang, 'all_on')}\n\n"
                f"{t(lang, 'my_feed_header', total=total, max=MAX_FREE_SOURCES)}"
            )
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb_my_feed(subs, lang))

        elif data == "feed_export":
            subs = self.db.query(
                "SELECT source_type, source_name, source_url, original_url, is_active, created_at "
                "FROM subscriptions WHERE user_id=%s ORDER BY created_at",
                (user["id"],),
            )
            if not subs:
                await query.answer(t(lang, "no_sources_export"), show_alert=True)
                return
            lines = ["Type,Name,URL,Status,Added"]
            for s in subs:
                status = "Active" if s["is_active"] else "Off"
                dt     = s["created_at"].strftime("%Y-%m-%d") if s["created_at"] else ""
                name   = (s["source_name"] or "").replace(",", ";")
                url    = (s["original_url"] or s["source_url"]).replace(",", ";")
                lines.append(f"{s['source_type']},{name},{url},{status},{dt}")
            csv_text = "\n".join(lines)
            buf      = io.BytesIO(csv_text.encode("utf-8"))
            buf.name = "data_sources.csv"
            await self.app.bot.send_document(
                chat_id=tg_id, document=buf, filename="data_sources.csv",
                caption=t(lang, "export_caption"),
            )
            await query.answer(t(lang, "exported"), show_alert=False)

        elif data.startswith("feed_noop_"):
            await query.answer(t(lang, "already_added"), show_alert=True)

    # ════════════════════════════════════════════════════════════════════════════
    # ADMIN CALLBACKS
    # ════════════════════════════════════════════════════════════════════════════

    async def _handle_admin_cb(self, query, data):
        if data == "adm_stats":
            u  = self.db.one("SELECT COUNT(*) AS c FROM users")
            s  = self.db.one("SELECT COUNT(*) AS c FROM subscriptions WHERE is_active=TRUE")
            p  = self.db.one("SELECT COUNT(*) AS c FROM posts")
            sh = self.db.one("SELECT COUNT(*) AS c FROM sent_history")
            ch = self.db.one("SELECT COUNT(*) AS c FROM channels")
            bn = self.db.one("SELECT COUNT(*) AS c FROM users WHERE is_banned=TRUE")
            br = self.db.one("SELECT COUNT(*) AS c FROM broadcast_log")
            nu = self.db.one(
                "SELECT COUNT(*) AS c FROM users WHERE created_at>=NOW()-INTERVAL '24 hours'"
            )
            top = self.db.query(
                "SELECT username, forwarded_count FROM users ORDER BY forwarded_count DESC LIMIT 5"
            )
            top_text = "\n".join(
                f"  {i+1}. @{r['username'] or '?'} — {r['forwarded_count']}"
                for i, r in enumerate(top)
            ) or "  (none yet)"
            await query.edit_message_text(
                f"📊 <b>Admin Stats</b>\n\n"
                f"👤 Total users:      {u['c']}\n"
                f"🆕 New today (24h):  {nu['c']}\n"
                f"🚫 Banned:          {bn['c']}\n"
                f"📡 Channels:         {ch['c']}\n"
                f"🔗 Active subs:      {s['c']}\n"
                f"📰 Posts cached:     {p['c']}\n"
                f"📤 Total forwarded:  {sh['c']}\n"
                f"📢 Broadcasts sent:  {br['c']}\n\n"
                f"<b>🏆 Top forwarders:</b>\n{top_text}",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[btn("◀️ Back", "adm_back")]]),
            )

        elif data == "adm_broadcast":
            self._set_state(OWNER_ID, "adm_broadcast", "")
            await query.edit_message_text(
                "📢 <b>Broadcast</b>\n\nType your message. Supports HTML.\n\nSend /cancel to abort.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[btn("◀️ Back", "adm_back")]]),
            )

        elif data == "adm_activity":
            rows = self.db.query(
                "SELECT u.username, p.title, p.source_type, sh.sent_at "
                "FROM sent_history sh JOIN users u ON sh.user_id=u.id "
                "JOIN posts p ON sh.post_id=p.id ORDER BY sh.sent_at DESC LIMIT 20"
            )
            if not rows:
                text = "📜 <b>No activity yet.</b>"
            else:
                text = "📜 <b>Last 20 deliveries</b>\n\n"
                for r in rows:
                    icon  = PLATFORMS.get(r["source_type"], "📱")
                    title = (r["title"] or "?")[:40]
                    uname = r["username"] or "?"
                    ts    = r["sent_at"].strftime("%m/%d %H:%M") if r["sent_at"] else "?"
                    text += f"{icon} @{uname} — {title} [{ts}]\n"
            await query.edit_message_text(
                text, parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[btn("◀️ Back", "adm_back")]]),
            )

        elif data == "adm_users":
            total_row = self.db.one("SELECT COUNT(*) AS c FROM users")
            rows = self.db.query(
                "SELECT telegram_id, username, forwarded_count, is_banned, created_at "
                "FROM users ORDER BY created_at DESC LIMIT 30"
            )
            text = f"👥 <b>Users (last 30 of {total_row['c']})</b>\n\n"
            for r in rows:
                ban   = "🚫" if r["is_banned"] else "✅"
                uname = r["username"] or "?"
                dt    = r["created_at"].strftime("%m/%d/%y") if r["created_at"] else "?"
                text += f"{ban} @{uname} <code>{r['telegram_id']}</code> | {r['forwarded_count']}⬆ | {dt}\n"
            await query.edit_message_text(
                text, parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[btn("◀️ Back", "adm_back")]]),
            )

        elif data == "adm_channels":
            rows = self.db.query(
                "SELECT c.chat_id, c.chat_title, c.chat_type, u.username, c.created_at "
                "FROM channels c JOIN users u ON c.owner_id=u.id ORDER BY c.created_at DESC LIMIT 30"
            )
            if not rows:
                text = "📡 <b>No channels yet.</b>"
            else:
                text = "📡 <b>Channels/Groups (last 30)</b>\n\n"
                for r in rows:
                    dt    = r["created_at"].strftime("%m/%d/%y") if r["created_at"] else "?"
                    text += (
                        f"• <b>{r['chat_title']}</b> [{r['chat_type']}]\n"
                        f"  @{r['username'] or '?'} | <code>{r['chat_id']}</code> | {dt}\n"
                    )
            await query.edit_message_text(
                text, parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[btn("◀️ Back", "adm_back")]]),
            )

        elif data == "adm_ban":
            self._set_state(OWNER_ID, "adm_ban", "")
            await query.edit_message_text(
                "🚫 <b>Ban user</b>\n\nSend the Telegram ID to ban. /cancel to abort.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[btn("◀️ Back", "adm_back")]]),
            )

        elif data == "adm_unban":
            self._set_state(OWNER_ID, "adm_unban", "")
            await query.edit_message_text(
                "✅ <b>Unban user</b>\n\nSend the Telegram ID to unban. /cancel to abort.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[btn("◀️ Back", "adm_back")]]),
            )

        elif data == "adm_force_poll":
            await query.edit_message_text("⚡ <b>Force poll running...</b>", parse_mode="HTML")
            try:
                await self._poll_all(self.app)
                await query.edit_message_text(
                    "⚡ <b>Force poll complete!</b>", parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([[btn("◀️ Back", "adm_back")]]),
                )
            except Exception as e:
                await query.edit_message_text(
                    f"❌ Poll error: {e}", parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([[btn("◀️ Back", "adm_back")]]),
                )

        elif data == "adm_clean":
            row = self.db.one(
                "SELECT COUNT(*) AS c FROM posts "
                "WHERE created_at < NOW()-INTERVAL '30 days' "
                "AND id NOT IN (SELECT post_id FROM sent_history)"
            )
            count = row["c"] if row else 0
            if count > 0:
                self.db.run(
                    "DELETE FROM posts WHERE created_at < NOW()-INTERVAL '30 days' "
                    "AND id NOT IN (SELECT post_id FROM sent_history)"
                )
            await query.edit_message_text(
                f"🗑️ <b>Cleanup complete</b>\n\nRemoved <b>{count}</b> old posts.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[btn("◀️ Back", "adm_back")]]),
            )

        elif data == "adm_banned_list":
            rows = self.db.query("SELECT telegram_id, username FROM users WHERE is_banned=TRUE")
            if not rows:
                text = "✅ <b>No banned users.</b>"
            else:
                text = f"🚫 <b>Banned users ({len(rows)})</b>\n\n"
                for r in rows:
                    text += f"• @{r['username'] or '?'} — <code>{r['telegram_id']}</code>\n"
            await query.edit_message_text(
                text, parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[btn("◀️ Back", "adm_back")]]),
            )

        elif data == "adm_back":
            await query.edit_message_text(
                "🛠️ <b>Admin Panel</b>", parse_mode="HTML", reply_markup=kb_admin(),
            )

    # ════════════════════════════════════════════════════════════════════════════
    # TEXT HANDLER
    # ════════════════════════════════════════════════════════════════════════════

    async def handle_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        tg_id       = update.effective_user.id
        text        = update.message.text.strip()
        state, _ctx = self._get_state(tg_id)
        user        = self.db.one("SELECT * FROM users WHERE telegram_id=%s", (tg_id,))
        lang        = get_lang(user)

        if text.lower() == "/cancel":
            self._clear_state(tg_id)
            await update.message.reply_text(t(lang, "cancelled"))
            return

        # Owner admin states
        if tg_id == OWNER_ID:
            if state == "adm_broadcast":
                self._clear_state(tg_id)
                await self._do_broadcast(update, text)
                return
            if state == "adm_ban":
                self._clear_state(tg_id)
                if not text.isdigit():
                    await update.message.reply_text("❌ Send a numeric Telegram ID.")
                    return
                target_id = int(text)
                u = self.db.one("SELECT id, username FROM users WHERE telegram_id=%s", (target_id,))
                if not u:
                    await update.message.reply_text("❌ User not found.")
                    return
                self.db.run("UPDATE users SET is_banned=TRUE WHERE telegram_id=%s", (target_id,))
                await update.message.reply_text(
                    f"🚫 Banned @{u['username'] or '?'} (<code>{target_id}</code>)", parse_mode="HTML"
                )
                return
            if state == "adm_unban":
                self._clear_state(tg_id)
                if not text.isdigit():
                    await update.message.reply_text("❌ Send a numeric Telegram ID.")
                    return
                target_id = int(text)
                self.db.run("UPDATE users SET is_banned=FALSE WHERE telegram_id=%s", (target_id,))
                await update.message.reply_text(
                    f"✅ Unbanned <code>{target_id}</code>", parse_mode="HTML"
                )
                return

        # add_source: user pasted a URL → show confirmation
        if state == "add_source":
            if not await self._gate(update, context):
                self._clear_state(tg_id)
                return
            raw_url = text.strip()
            src_type, handle_or_url, original_url, needs_check = detect_platform(raw_url)
            if not src_type or not handle_or_url:
                await update.message.reply_text(t(lang, "url_not_recognised"))
                return
            # BUG FIX: store URL in "confirm_url" state, not "add_source"
            self._set_state(tg_id, "confirm_url", raw_url)
            display_name = original_url or handle_or_url
            platform_label = src_type.replace("_handle", "").upper()
            await update.message.reply_text(
                f"<b>{platform_label} Source</b>\n"
                f"<a href='{display_name}'>{display_name[:80]}</a>",
                parse_mode="HTML",
                disable_web_page_preview=False,
                reply_markup=kb_add_source_confirm(lang),
            )
            return

        elif state == "set_kw":
            self._clear_state(tg_id)
            self.db.run(
                "UPDATE users SET keyword_filter=%s WHERE telegram_id=%s", (text[:500], tg_id)
            )
            await update.message.reply_text(t(lang, "keywords_saved"))

    # ════════════════════════════════════════════════════════════════════════════
    # CHAT MEMBER
    # ════════════════════════════════════════════════════════════════════════════

    async def on_chat_member(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        result     = update.my_chat_member
        chat       = result.chat
        new_status = result.new_chat_member.status
        added_by   = result.from_user

        if new_status in ("administrator", "member") and added_by:
            try:
                user = self._ensure_user(added_by.id, added_by.username or "user")
                self.db.run(
                    "INSERT INTO channels (owner_id, chat_id, chat_title, chat_type) "
                    "VALUES (%s,%s,%s,%s) ON CONFLICT (chat_id) DO UPDATE "
                    "SET chat_title=EXCLUDED.chat_title",
                    (user["id"], chat.id, chat.title or "Unnamed", chat.type),
                )
                await context.bot.send_message(
                    chat_id=chat.id,
                    text="✅ <b>IN365Bot connected!</b>\n\nUse /add in private chat to subscribe.",
                    parse_mode="HTML",
                )
                await self._notify_owner(
                    f"📡 <b>Bot added to channel/group!</b>\n\n"
                    f"📌 <b>{chat.title}</b> [{chat.type}]\n"
                    f"🆔 <code>{chat.id}</code>\n"
                    f"👤 Added by: @{added_by.username or '?'} (<code>{added_by.id}</code>)\n"
                    f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC"
                )
            except Exception as e:
                logger.error(f"on_chat_member add: {e}")

        elif new_status in ("left", "kicked"):
            try:
                await self._notify_owner(
                    f"⚠️ <b>Bot removed from channel/group!</b>\n\n"
                    f"📌 <b>{chat.title}</b> [{chat.type}]\n"
                    f"🆔 <code>{chat.id}</code>\n"
                    f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC"
                )
            except Exception as e:
                logger.error(f"on_chat_member remove notify: {e}")

    # ════════════════════════════════════════════════════════════════════════════
    # POLLING LOOP
    # ════════════════════════════════════════════════════════════════════════════

    async def _polling_loop(self, app):
        while True:
            try:
                await self._poll_all(app)
            except Exception as e:
                logger.error(f"Polling loop: {e}")
            await asyncio.sleep(POLL_INTERVAL)

    async def _poll_all(self, app):
        user_subs = self.db.query(
            "SELECT s.id, s.source_url, s.source_type, s.source_name, "
            "u.telegram_id, u.id AS user_id, u.silent_mode, u.keyword_filter, "
            "u.hide_original_link, u.moderation_mode, u.language, "
            "NULL::bigint AS channel_chat_id, NULL::int AS channel_id "
            "FROM subscriptions s JOIN users u ON s.user_id=u.id "
            "WHERE s.user_id IS NOT NULL AND s.is_active=TRUE AND u.is_banned=FALSE "
            "ORDER BY s.last_check ASC NULLS FIRST LIMIT 30"
        )
        chan_subs = self.db.query(
            "SELECT s.id, s.source_url, s.source_type, s.source_name, "
            "NULL::bigint AS telegram_id, NULL::int AS user_id, "
            "FALSE AS silent_mode, '' AS keyword_filter, "
            "FALSE AS hide_original_link, FALSE AS moderation_mode, 'en' AS language, "
            "c.chat_id AS channel_chat_id, c.id AS channel_id "
            "FROM subscriptions s JOIN channels c ON s.channel_id=c.id "
            "WHERE s.channel_id IS NOT NULL AND s.is_active=TRUE "
            "ORDER BY s.last_check ASC NULLS FIRST LIMIT 30"
        )
        all_subs = list(user_subs) + list(chan_subs)
        if not all_subs:
            return
        for sub in all_subs:
            try:
                posts = await asyncio.to_thread(fetch_feed, sub["source_url"], sub["source_type"], 5)
                for post in posts:
                    await self._deliver(app, sub, post)
                self.db.run("UPDATE subscriptions SET last_check=NOW() WHERE id=%s", (sub["id"],))
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Poll sub {sub['id']}: {e}")

    async def _deliver(self, app, sub, post):
        try:
            kw = (sub.get("keyword_filter") or "").strip()
            if kw:
                kws = [k.strip().lower() for k in kw.split(",") if k.strip()]
                if any(k in post["title"].lower() for k in kws):
                    return

            if sub.get("moderation_mode"):
                bad = ["18+", "nsfw", "adult", "xxx", "porn", "casino", "gambling"]
                if any(w in post["title"].lower() for w in bad):
                    return

            ex = self.db.one("SELECT id FROM posts WHERE source_id=%s", (post["source_id"],))
            if ex:
                post_id = ex["id"]
            else:
                post_id = self.db.insert_id(
                    "INSERT INTO posts (source_id, source_type, title, url, media_url) "
                    "VALUES (%s,%s,%s,%s,%s) RETURNING id",
                    (post["source_id"], post["source_type"],
                     post["title"][:500], post["url"][:500],
                     (post.get("media_url") or "")[:500]),
                )
                if post_id is None:
                    return

            user_id    = sub.get("user_id")
            channel_id = sub.get("channel_id")
            lang       = sub.get("language") or "en"

            if user_id:
                sent = self.db.one(
                    "SELECT id FROM sent_history WHERE user_id=%s AND post_id=%s",
                    (user_id, post_id),
                )
            else:
                sent = self.db.one(
                    "SELECT id FROM sent_history WHERE channel_id=%s AND post_id=%s",
                    (channel_id, post_id),
                )
            if sent:
                return

            icon        = PLATFORMS.get(post["source_type"], "📱")
            source_name = (sub.get("source_name") or post["source_type"])[:60]
            hide        = bool(sub.get("hide_original_link", False))
            msg_text    = f"{icon} <b>{source_name}</b>\n\n{post['title']}"
            if not hide:
                msg_text += f"\n\n<a href='{post['url']}'>{t(lang, 'view_original')}</a>"

            target = sub.get("telegram_id") or sub.get("channel_chat_id")
            if not target:
                return

            await app.bot.send_message(
                chat_id=target, text=msg_text, parse_mode="HTML",
                disable_notification=bool(sub.get("silent_mode", False)),
                disable_web_page_preview=False,
            )

            self.db.run(
                "INSERT INTO sent_history (user_id, channel_id, post_id) VALUES (%s,%s,%s) "
                "ON CONFLICT DO NOTHING",
                (user_id, channel_id, post_id),
            )
            if user_id:
                self.db.run(
                    "UPDATE users SET forwarded_count=forwarded_count+1 WHERE id=%s", (user_id,)
                )

        except TelegramError as e:
            logger.warning(f"Telegram error → {sub.get('telegram_id') or sub.get('channel_chat_id')}: {e}")
        except Exception as e:
            logger.error(f"Deliver: {e}")

    def run(self):
        logger.info("IN365Bot v2.4 starting...")
        try:
            self.app.run_polling(
                allowed_updates=["message", "callback_query", "my_chat_member"]
            )
        finally:
            if self.db.conn:
                self.db.conn.close()
            logger.info("Bot stopped.")


if __name__ == "__main__":
    Bot().run()