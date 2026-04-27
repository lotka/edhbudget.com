function updateDeckId(table, id) {
  const budget = Number(table.dataset.budget);
  const loadingImage = table.dataset.loadingImage;

  $("#deck-" + id + "-refresh").html('<img src="' + loadingImage + '">');
  $.post("/update_deck_id", {"id": id}).done(function(response) {
    const deckPriceSeason = response["deck_price_season"];
    const priceClass = deckPriceSeason > budget ? "price-status--over" : "price-status--under";
    $("#deck-" + id + "-deck_price-season").html(
      '<span class="price-status ' + priceClass + '"> $' + deckPriceSeason + "</span>"
    );
    $("#deck-" + id + "-deck_price-season-new").text("$" + response["deck_price_season_new"]);
    $("#deck-" + id + "-commander_price").text(response["commander_price"]);
    $("#deck-" + id + "-modified").text(response["modified"].substr(0, 16));
    $("#deck-" + id + "-refresh").text("🔄");
  }).fail(function(response) {
    $("#deck-" + id + "-refresh").text("⚠️");
    console.log(response);
  });
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function updateAllDecks() {
  const table = document.querySelector(".deck-list-table");
  const decks = $(".deck-row").toArray();
  for (let i = 0; i < decks.length; ++i) {
    updateDeckId(table, decks[i].dataset.deckId);
    await sleep(100);
  }
}

$(function() {
  const table = document.querySelector(".deck-list-table");
  if (!table) {
    return;
  }

  $(".deck-refresh").on("click", function(event) {
    event.preventDefault();
    updateDeckId(table, this.dataset.deckId);
  });

  $(".update-all-decks").on("click", function(event) {
    event.preventDefault();
    updateAllDecks();
  });
});
