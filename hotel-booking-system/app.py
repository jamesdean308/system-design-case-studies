from flask import Flask, request, jsonify
import sqlite3

app = Flask(__name__)

# Initialize a SQLite database
db = sqlite3.connect("hotel.db")
cursor = db.cursor()
cursor.execute(
    "CREATE TABLE IF NOT EXISTS Room (RoomID INTEGER PRIMARY KEY, RoomNumber TEXT, RoomType TEXT, PricePerNight REAL, Version INTEGER);"
)
cursor.execute(
    "CREATE TABLE IF NOT EXISTS Booking (BookingID INTEGER PRIMARY KEY, RoomID INTEGER, GuestName TEXT, CheckInDate DATE, CheckOutDate DATE);"
)
db.commit()


@app.route("/book_room", methods=["POST"])
def book_room():
    """
    Books a hotel room with optimistic locking.

    This function books a hotel room for a guest while utilizing optimistic locking to prevent double bookings
    and ensure data consistency. It checks the current version of the room to ensure no concurrent updates have
    occurred since the user last retrieved the room's version. If the versions match, the booking is allowed,
    and the room's version is updated. If a conflict is detected, the function returns a 409 Conflict status
    along with an error message.

    Args:
        JSON data (POST request):
            - room_id: The unique identifier of the room to be booked.
            - guest_name: The name of the guest booking the room.
            - checkin_date: The check-in date.
            - checkout_date: The check-out date.
            - user_retrieved_version: The version of the room last retrieved by the user.

    Returns:
        JSON response:
            - If the booking is successful, it returns a success message.
            - If a conflict is detected, it returns a 409 Conflict status with an error message.
            - If an error occurs, it returns a 500 Internal Server Error status with an error message.

    Usage:
        You can use this endpoint to book hotel rooms while ensuring data consistency and preventing double bookings.

    """
    data = request.json
    room_id = data["room_id"]
    guest_name = data["guest_name"]
    checkin_date = data["checkin_date"]
    checkout_date = data["checkout_date"]

    try:
        db = sqlite3.connect("hotel.db")
        cursor = db.cursor()

        # Retrieve the current version of the room
        cursor.execute("SELECT Version FROM Room WHERE RoomID = ?", (room_id,))
        current_version = cursor.fetchone()[0]

        # Check if the current version matches the version the user retrieved earlier
        if current_version == data["user_retrieved_version"]:
            # If the version matches, insert the booking and update the version
            cursor.execute(
                "INSERT INTO Booking (RoomID, GuestName, CheckInDate, CheckOutDate) VALUES (?, ?, ?, ?)",
                (room_id, guest_name, checkin_date, checkout_date),
            )
            cursor.execute(
                "UPDATE Room SET Version = ? WHERE RoomID = ?",
                (current_version + 1, room_id),
            )
            db.commit()
            return jsonify({"message": "Booking successful."})

        else:
            # Handle the case where the version doesn't match (a conflict occurred)
            return (
                jsonify(
                    {
                        "error": "Conflict: The room has been booked by someone else in the meantime."
                    }
                ),
                409,
            )

    except Exception as e:
        db.rollback()
        return jsonify({"error": "An error occurred while booking the room."}), 500

    finally:
        db.close()


if __name__ == "__main__":
    app.run(debug=True)
