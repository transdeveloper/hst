const express = require('express');
const { WebSocketServer } = require('ws');
const { MongoClient } = require('mongodb');

const app = express();
const PORT = 3000;
// const WS_PORT = 8080; // unused old var
const RESULT_LIMIT = 50;
const MAX_PAGE = 100;

// MongoDB Setup, we VILL hardcore
const mongoUri = "mongodb://root:password@172.18.0.106:27017";
const client = new MongoClient(mongoUri);
// password
let productNamesCollection;
let productsCollection;

// Connect to MongoDB once when the server starts
async function initMongo() {
  try {
	await client.connect();
	const db = client.db("off");
	productNamesCollection = db.collection("product_names");
	productsCollection = db.collection("products");
	console.log("Connected to MongoDB");
  } catch (err) {
	console.error("MongoDB Connection Error:", err);
	process.exit(1);
  }
}

app.use(express.json());

app.get('/status', (req, res) => {
  res.json({ status: "Server is running smoothly" });
});

app.get('/', (req, res) => {
  // send index.html
  res.sendFile(__dirname + '/cart.html');
});

// app.get('/cart', (req, res) => {
//   res.sendFile(__dirname + '/cart.html');
// });

const fs = require('fs').promises;
const path = require('path');
const CART_FILE = path.join(__dirname, "cart.json");

// really basic, honestly we dont really need anything fancy its only to be used at home
app.put("/cart.json", async (req, res) => {
  try {
	await fs.writeFile(
		CART_FILE,
		JSON.stringify(req.body, null, 2),
		"utf8"
	);

	res.json({
	  success: true
	});

  } catch (err) {
	console.error(err);
	res.status(500).json({
	  success: false
	});
  }
});


// same here bozo
app.get("/cart.json", async (req, res) => {
  try {
	const text = await fs.readFile(CART_FILE, "utf8");
	res.json(JSON.parse(text));
  } catch (err) {
	// File doesn't exist yet
	if (err.code === "ENOENT") {
	  return res.json({});
	}
	console.error(err);
	res.status(500).json({
	  error: "Failed to load cart."
	});
  }
});

const http = require('http');
const server = http.createServer(app);

const wss = new WebSocketServer({
  server
});

server.listen(PORT, () => {
  console.log(`HTTP server running on http://localhost:${PORT}`);
  console.log(`WebSocket server running on ws://localhost:${PORT}`);
});

// app.listen(PORT, () => {
//   console.log(`Express web server running on http://localhost:${PORT}`);
// });
//
// const wss = new WebSocketServer({ port: WS_PORT }, () => {
//   console.log(`WebSocket server running on ws://localhost:${WS_PORT}`);
// });

wss.on('connection', (ws) => {
  console.log('Client connected via WebSocket');

  // Handle incoming query messages from clients
  ws.on('message', async (message) => {
	try {
	  // We are expecting JSON data like: { "searchTerms": ["peanut", "butter"] }
	  const data = JSON.parse(message);

	  if (data.cart !== undefined) {
		await fs.writeFile(
			CART_FILE,
			JSON.stringify(data.cart, null, 2),
			"utf8"
		);
		wss.clients.forEach(client => {
		  if (client !== ws && client.readyState === WebSocket.OPEN) {
			client.send(JSON.stringify({ type: "CART_UPDATED", cart: data.cart }));
		  }
		})
		return;
	  }


	  if (!data.searchTerms || !Array.isArray(data.searchTerms)) {
		ws.send(JSON.stringify({ error: "Missing or invalid 'searchTerms' array" }));
		return;
	  }

	  const searchTerms = normalizeSearchTerms(data.searchTerms);
	  const page = normalizePage(data.page);
	  const skip = (page - 1) * RESULT_LIMIT;

	  if (searchTerms.length === 0) {
		ws.send(JSON.stringify({
		  type: "QUERY_RESULTS",
		  requestId: data.requestId,
		  page,
		  pageSize: RESULT_LIMIT,
		  hasMore: false,
		  count: 0,
		  data: []
		}));
		return;
	  }

	  // Build the query
	  const conditions = searchTerms.map(term => ({
		$or: [
		  { _id: term },
		  { code: term },
		  { _id: new RegExp(`^${escapeRegex(term)}`) },
		  { code: new RegExp(`^${escapeRegex(term)}`) },
		  { _keywords: term },
		  { product_name: new RegExp(`^${escapeRegex(term)}`, "i") }
		]
	  }));

	  const query = {
		countries_tags: "en:united-kingdom",
		// uuu this is important otherwise too may results, best to keep it geo-specific
		// potentially impl maxmind to determine location and have a selection on the ui
		$and: conditions
	  };

	  // Fetch results from MongoDB
	  const results = await productNamesCollection.find(query, {
		projection: {
		  _id: 1,
		  code: 1,
		  product_name: 1,
		  brands: 1,
		  quantity: 1,
		  countries_tags: 1,
		  _keywords: 1,
		  image_url: 1,
		  image_front_url: 1,
		  image_front_small_url: 1,
		  image_front_thumb_url: 1
		},
		skip,
		limit: RESULT_LIMIT + 1,
		maxTimeMS: 30000
	  }).toArray();
	  const pageResults = results.slice(0, RESULT_LIMIT);
	  const hasMore = results.length > RESULT_LIMIT;
	  const imageMetadataByBarcode = await loadImageMetadataByBarcode(pageResults);

	  const products = pageResults.map(productName => {
		const barcode = getProductBarcode(productName);
		const fullProduct = barcode ? imageMetadataByBarcode.get(barcode) : null;

		return {
		  ...productName,
		  awsImageUrl: buildAwsS3ImageUrl(fullProduct, "front"),
		  ingredients_text_with_allergens:
			  fullProduct?.ingredients_text_with_allergens ?? null,
		  nutrition_grade: fullProduct.nutriscore_data?.grade ?? null
		};
	  });

	  // const products = pageResults.map(productName => {
		// const barcode = getProductBarcode(productName);
		// const fullProduct = barcode ? imageMetadataByBarcode.get(barcode) : null;
		// return {
		//   ...productName,
		//   awsImageUrl: buildAwsS3ImageUrl(fullProduct, "front")
		//   // awsImageUrl: fullProduct ? buildAwsS3ImageUrl(fullProduct, "front") : buildFastImageUrl(productName)
		// };
	  // });

	  // we send le response back to this specific client
	  ws.send(JSON.stringify({
		type: "QUERY_RESULTS",
		requestId: data.requestId,
		page,
		pageSize: RESULT_LIMIT,
		hasMore,
		count: products.length,
		data: products
	  }));

	} catch (err) {
	  console.error("Error processing WebSocket message:", err);
	  ws.send(JSON.stringify({ error: "Failed to process query or invalid JSON format" }));
	}
  });

  ws.on('close', () => {
	console.log('Client disconnected');
  });
});

// Start MongoDB connection
initMongo();

function buildAwsS3ImageUrl(product, view) {
  const barcode = getProductBarcode(product);

  if (!barcode)
	return null;

  const images = product.images?.selected?.[view];

  if (!images)
	return null;

  // Prefer en, otherwise use the first available language
  const image = images.en ?? Object.values(images).find(img => img?.imgid);

  if (!image?.imgid)
	return null;

  return `https://openfoodfacts-images.s3.eu-west-3.amazonaws.com/data/${toOpenFoodFactsBarcodePath(barcode)}/${image.imgid}.400.jpg`;
}

async function loadImageMetadataByBarcode(productNames) {
  const barcodes = [...new Set(productNames.map(getProductBarcode).filter(Boolean))];

  if (barcodes.length === 0) return new Map();

  try {
	const products = await productsCollection.find(
	  { _id: { $in: barcodes } },
	  {
		projection: {
		  _id: 1,
		  code: 1,
		  images: 1,
		  ingredients_text_with_allergens: 1,
		  nutriscore_data: 1
		},
		maxTimeMS: 30000
	  }
	).toArray();

	return new Map(
	  products
		.map(product => [getProductBarcode(product), product])
		.filter(([barcode]) => barcode)
	);
  } catch (err) {
	console.warn("Skipping product image metadata lookup:", err.message);
	return new Map();
  }
}

function getProductBarcode(product) {
  if (product.code) return cleanBarcode(product.code);

  if (typeof product._id === 'string' || typeof product._id === 'number') {
	return cleanBarcode(product._id);
  }

  return null;
}

function cleanBarcode(value) {
  const barcode = String(value || '').replace(/\D/g, '');
  return barcode.length > 0 ? barcode : null;
}

function toOpenFoodFactsBarcodePath(barcode) {
  barcode = String(barcode).padStart(13, "0");

  return [
	barcode.slice(0, 3),
	barcode.slice(3, 6),
	barcode.slice(6, 9),
	barcode.slice(9)
  ].join("/");
}

function normalizeSearchTerms(searchTerms) {
  return [...new Set(searchTerms
	.map(term => String(term || '').trim().toLowerCase())
	.filter(term => term.length >= 2)
	.slice(0, 6)
  )];
}

function normalizePage(page) {
  const value = Number.parseInt(page, 10);

  if (!Number.isFinite(value) || value < 1) return 1;

  return Math.min(value, MAX_PAGE);
}

function escapeRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
