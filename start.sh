while true; do
    python3 bot_manager_server.py
    if [ $? -ne 5 ]; then
        break
    fi
    echo "Перезапуск скрипта..."
    sleep 1
done