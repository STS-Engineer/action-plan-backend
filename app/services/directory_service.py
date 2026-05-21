from app.models.company_member import CompanyMember


def normalize_email(email: str | None):
    return email.strip().lower() if email else None


def get_member_by_email(db, email: str):
    email = normalize_email(email)

    if not email:
        return None

    return (
        db.query(CompanyMember)
        .filter(CompanyMember.email.ilike(email))
        .first()
    )


def get_direct_reports(db, manager_email: str):
    manager_email = normalize_email(manager_email)

    if not manager_email:
        return []

    return (
        db.query(CompanyMember)
        .filter(CompanyMember.manager_email.ilike(manager_email))
        .order_by(CompanyMember.display_name.asc())
        .all()
    )


def get_underlings_until_depth(db, manager_email: str, max_depth: int | None = None):
    manager_email = normalize_email(manager_email)

    if not manager_email:
        return []

    result = []
    visited_managers = set()
    seen_members = set()
    queue = [(manager_email, 0)]

    while queue:
        current_manager_email, depth = queue.pop(0)

        if current_manager_email in visited_managers:
            continue

        visited_managers.add(current_manager_email)

        if max_depth is not None and depth >= max_depth:
            continue

        direct_reports = get_direct_reports(db, current_manager_email)

        for member in direct_reports:
            member_email = normalize_email(member.email)
            member_key = member_email or f"id:{member.id}"

            if member_key not in seen_members:
                seen_members.add(member_key)
                result.append(member)

            if member_email:
                queue.append((member_email, depth + 1))

    return result


def get_all_underlings(db, manager_email: str):
    return get_underlings_until_depth(db, manager_email)


def get_manager_chain(db, email: str):
    member = get_member_by_email(db, email)

    if not member:
        return []

    chain = []
    visited = set()

    current = member

    while current and current.manager_email:
        manager_email = normalize_email(current.manager_email)

        if manager_email in visited:
            break

        visited.add(manager_email)

        manager = get_member_by_email(db, manager_email)

        if not manager:
            break

        chain.append(manager)
        current = manager

    return chain
