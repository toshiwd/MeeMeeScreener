# ブラウザの開発者コンソールで実行してください

# スクリーナーAPIを呼び出して、1928のデータを確認
fetch('/api/screener')
    .then(r => r.json())
    .then(data => {
        const sekisui = data.items.find(item => item.code === '1928');
        if (sekisui) {
            console.log('=== 1928 (Sekisui House) ===');
            console.log('eventEarningsDate:', sekisui.eventEarningsDate);
            console.log('eventRightsDate:', sekisui.eventRightsDate);
            console.log('event_earnings_date:', sekisui.event_earnings_date);
            console.log('event_rights_date:', sekisui.event_rights_date);
            console.log('\nFull data:', sekisui);
        } else {
            console.log('1928 not found');
        }
    });
