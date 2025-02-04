import time

def analyze_student_data():
    student_scores = [
        {"student_id": 1, "name": "John", "scores": [75, 82, 91, 65, 88]},
        {"student_id": 2, "name": "Emma", "scores": [95, 88, 92, 87, 90]},
        {"student_id": 3, "name": "Michael", "scores": [70, 65, 88, 92, 85]},
        {"student_id": 4, "name": "Sarah", "scores": [89, 91, 95, 88, 85]},
        {"student_id": 5, "name": "James", "scores": [78, 85, 91, 87, 83]},
    ] * 2000  

    start_time = time.time()

    student_averages = []
    for student in student_scores:
        total = 0
        for score in student["scores"]:
            total = total + score
        average = total / len(student["scores"])
        student_averages.append({"student_id": student["student_id"], "average": average})

    # Inefficient way to find top performers
    top_students = []
    for student in student_averages:
        if student["average"] > 85:
            for original_student in student_scores:
                if original_student["student_id"] == student["student_id"]:
                    top_students.append({
                        "name": original_student["name"],
                        "average": student["average"]
                    })

    # calculate class statistics
    all_scores = []
    for student in student_scores:
        for score in student["scores"]:
            all_scores.append(score)

    total_class_score = 0
    for score in all_scores:
        total_class_score += score
    
    class_average = total_class_score / len(all_scores)

    # grade distribution calculation
    grade_counts = {"A": 0, "B": 0, "C": 0, "D": 0, "F": 0}
    for score in all_scores:
        if score >= 90:
            grade_counts["A"] = grade_counts["A"] + 1
        elif score >= 80:
            grade_counts["B"] = grade_counts["B"] + 1
        elif score >= 70:
            grade_counts["C"] = grade_counts["C"] + 1
        elif score >= 60:
            grade_counts["D"] = grade_counts["D"] + 1
        else:
            grade_counts["F"] = grade_counts["F"] + 1

    execution_time = time.time() - start_time
    
    return {
        "student_averages": student_averages,
        "top_students": top_students,
        "class_average": class_average,
        "grade_distribution": grade_counts,
        "execution_time": execution_time
    }

# Run the unoptimized version
result = analyze_student_data()
print(f"Unoptimized Execution Time: {result['execution_time']:.4f} seconds")



